# bot_telegram_marketplace.py (corregido)

import asyncio
import os
import sqlite3
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Bot
from scraper_marketplace import main_scraper
from telegram.helpers import escape_markdown
from utils_analisis import (
    inicializar_tabla_anuncios,
    limpiar_link,
    es_extranjero,
    SCORE_MIN_DB,
    ROI_MINIMO,
    modelos_bajo_rendimiento,
    MODELOS_INTERES,
    escapar_multilinea,
    validar_coherencia_precio_año,
    Config,
    get_db_connection
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
inicializar_tabla_anuncios()

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["CHAT_ID"].strip())
DB_PATH = os.environ.get("DB_PATH", "upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
bot = Bot(token=BOT_TOKEN)

async def safe_send(text: str, parse_mode="MarkdownV2"):
    for _ in range(3):
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=escapar_multilinea(text),
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Error enviando a Telegram (reintento): {e}")
            await asyncio.sleep(1)

def dividir_y_enviar(titulo: str, items: list[str]) -> list[str]:
    if not items:
        return []
    texto = titulo + "\n\n" + "\n\n".join(items)
    bloques = [texto[i:i+3000] for i in range(0, len(texto), 3000)]
    return bloques

async def buscar_autos_marketplace(modelos_override=None):
    """
    Función adaptada para compatibilidad con el scraper actual
    """
    brutos = []
    pendientes = []
    
    # Ejecutar el scraper para cada modelo
    modelos = modelos_override or MODELOS_INTERES
    
    for modelo in modelos:
        logger.info(f"Procesando modelo: {modelo}")
        
        # Configurar variable de entorno para el modelo
        os.environ["MODELO_OBJETIVO"] = modelo
        
        try:
            # Ejecutar el scraper
            await main_scraper()
            
            # Obtener los resultados de la base de datos
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT link, modelo, anio, precio, roi, score, relevante, motivo, descripcion
                    FROM anuncios 
                    WHERE modelo = ? 
                    AND datetime(fecha_creacion) >= datetime('now', '-1 hour')
                    ORDER BY score DESC, roi DESC
                """, (modelo,))
                
                resultados = cur.fetchall()
                
                for row in resultados:
                    link, modelo_db, anio, precio, roi, score, relevante, motivo, descripcion = row
                    
                    # Crear texto simulando el formato original
                    texto_simulado = f"""
                    {modelo_db.title()} {anio}
                    Precio: Q{precio:,}
                    {descripcion or ''}
                    Link: {link}
                    """
                    
                    brutos.append(texto_simulado)
                    
        except Exception as e:
            logger.error(f"Error procesando modelo {modelo}: {e}")
            pendientes.append(f"❌ Error en {modelo}: {str(e)}")
    
    return brutos, pendientes, []

async def enviar_ofertas():
    logger.info("📡 Iniciando bot de Telegram")
    now_local = datetime.now(ZoneInfo("America/Guatemala"))

    bajos = modelos_bajo_rendimiento()
    activos = [m for m in MODELOS_INTERES if m not in bajos]
    logger.info(f"✅ Modelos activos: {activos}")

    try:
        brutos, pendientes, _ = await buscar_autos_marketplace(modelos_override=activos)
    except Exception as e:
        logger.error(f"❌ Error en scraper: {e}")
        await safe_send("❌ Error ejecutando scraper, revisa logs.")
        return

    buenos, potenciales = [], []
    resumen_relevantes, resumen_potenciales = [], []
    motivos = {
        "incompleto": 0,
        "extranjero": 0,
        "modelo no detectado": 0,
        "año fuera de rango": 0,
        "precio fuera de rango": 0,
        "precio-año incoherente": 0,
        "roi bajo": 0
    }

    # Procesar resultados desde la base de datos directamente
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT link, modelo, anio, precio, roi, score, relevante, motivo
            FROM anuncios 
            WHERE datetime(fecha_creacion) >= datetime('now', '-1 hour')
            ORDER BY score DESC, roi DESC
        """)
        
        resultados = cur.fetchall()
        
        for row in resultados:
            link, modelo, anio, precio, roi, score, relevante, motivo = row
            
            logger.info(f"📅 Año detectado: {anio}")
            logger.info(f"💰 Precio detectado: Q{precio:,}")

            if not validar_coherencia_precio_año(precio, anio):
                motivos["precio-año incoherente"] += 1
                continue

            mensaje = (
                f"🚘 *{modelo.title()}*\n"
                f"• Año: {anio}\n"
                f"• Precio: Q{precio:,}\n"
                f"• ROI: {roi:.1f}%\n"
                f"• Score: {score}/10\n"
                f"🔗 {link}"
            )

            if relevante:
                buenos.append(mensaje)
                resumen_relevantes.append((modelo, link, roi, score))
            elif score >= SCORE_MIN_DB and roi >= ROI_MINIMO:
                potenciales.append(mensaje)
                resumen_potenciales.append((modelo, link, roi, score))
            else:
                # Categorizar motivos de descarte
                if "score" in motivo.lower():
                    motivos["precio fuera de rango"] += 1
                elif "roi" in motivo.lower():
                    motivos["roi bajo"] += 1
                elif "modelo" in motivo.lower():
                    motivos["modelo no detectado"] += 1
                else:
                    motivos["incompleto"] += 1

            logger.info(
                f"🔍 {modelo} | Año {anio} | Precio {precio} | ROI {roi:.1f}% | Score {score}/10 | Relevante: {relevante}"
            )

    total = len(resultados)
    await safe_send(f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}")

    desc_total = sum(motivos.values())
    if desc_total:
        detalles = "\n".join(f"• {k}: {v}" for k, v in motivos.items() if v)
        await safe_send(f"📉 Descartados:\n{detalles}")

    if not buenos and not potenciales:
        if now_local.hour == 18:
            await safe_send(f"📡 Ejecución a las {now_local.strftime('%H:%M')}, sin ofertas.")
        return

    for bloque in dividir_y_enviar("📦 *Ofertas destacadas:*", buenos):
        await safe_send(bloque)

    for bloque in dividir_y_enviar("🟡 *Potenciales (score≥4 & ROI≥10):*", potenciales):
        await safe_send(bloque)

    for bloque in dividir_y_enviar("📌 *Pendientes manuales:*", pendientes):
        await safe_send(bloque)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total_db = cur.fetchone()[0]
        await safe_send(f"📦 Total acumulado en base: {total_db} anuncios")

    logger.info("\n📋 Resumen final del scraping (para revisión manual):")
    logger.info(f"Guardados totales: {len(buenos) + len(potenciales)}")
    logger.info(f"Relevantes: {len(resumen_relevantes)}")
    logger.info(f"Potenciales: {len(resumen_potenciales)}")

    logger.info("\n🟢 Relevantes:")
    for modelo, url, roi, score in resumen_relevantes:
        logger.info(f"• {modelo.title()} | ROI: {roi:.1f}% | Score: {score}/10 → {url}")

    logger.info("\n🟡 Potenciales:")
    for modelo, url, roi, score in resumen_potenciales:
        logger.info(f"• {modelo.title()} | ROI: {roi:.1f}% | Score: {score}/10 → {url}")

if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
