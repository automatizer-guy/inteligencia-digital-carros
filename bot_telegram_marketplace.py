# bot_telegram_marketplace.py (mejorado y completo)

import asyncio
import os
import sqlite3
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Bot
from scraper_marketplace import buscar_autos_marketplace
from telegram.helpers import escape_markdown
from utils_analisis import (
    inicializar_tabla_anuncios, analizar_mensaje, limpiar_link, es_extranjero,
    SCORE_MIN_DB, SCORE_MIN_TELEGRAM, ROI_MINIMO,
    modelos_bajo_rendimiento, MODELOS_INTERES, escapar_multilinea,
    validar_coherencia_precio_año, DEBUG, get_estadisticas_db
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
    """Envía mensaje con reintentos automáticos"""
    for intento in range(3):
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=escapar_multilinea(text),
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Error enviando a Telegram (intento {intento+1}/3): {e}")
            if intento < 2:  # No esperar en el último intento
                await asyncio.sleep(1)
    logger.error("❌ No se pudo enviar mensaje después de 3 intentos")
    return None

def dividir_y_enviar(titulo: str, items: list[str]) -> list[str]:
    """Divide mensajes largos en bloques para Telegram"""
    if not items:
        return []
    texto = titulo + "\n\n" + "\n\n".join(items)
    bloques = [texto[i:i+3000] for i in range(0, len(texto), 3000)]
    return bloques

async def enviar_ofertas():
    """Función principal del bot"""
    logger.info("📡 Iniciando bot de Telegram")
    now_local = datetime.now(ZoneInfo("America/Guatemala"))

    # Obtener modelos activos (excluyendo los de bajo rendimiento)
    bajos = modelos_bajo_rendimiento()
    activos = [m for m in MODELOS_INTERES if m not in bajos]
    logger.info(f"✅ Modelos activos: {activos}")
    
    if bajos:
        logger.info(f"⚠️ Modelos pausados por bajo rendimiento: {bajos}")

    try:
        brutos, pendientes, alertas = await buscar_autos_marketplace(modelos_override=activos)
    except Exception as e:
        logger.error(f"❌ Error en scraper: {e}")
        await safe_send("❌ Error ejecutando scraper, revisa logs.")
        return

    # Si hay alertas críticas del scraper
    if alertas:
        for alerta in alertas:
            await safe_send(alerta)
        if any("sesión" in alerta.lower() for alerta in alertas):
            return  # No continuar si hay problemas de sesión

    buenos, potenciales = [], []
    resumen_relevantes, resumen_potenciales = [], []
    motivos = {
        "incompleto": 0,
        "extranjero": 0,
        "modelo_no_detectado": 0,
        "año_fuera_rango": 0,
        "precio_fuera_rango": 0,
        "precio_año_incoherente": 0,
        "roi_bajo": 0,
        "negativos": 0,
        "duplicados": 0
    }

    urls_procesadas = set()  # Para evitar duplicados
    textos_procesados = set()  # Para evitar duplicados por contenido similar

    for txt in brutos:
        if DEBUG:
            logger.info(f"\n📝 TEXTO CRUDO:\n{txt[:500]}")
        
        # NUEVO: Filtrar duplicados por contenido similar
        txt_hash = hash(txt[:200])  # Hash de los primeros 200 caracteres
        if txt_hash in textos_procesados:
            motivos["duplicados"] += 1
            continue
        textos_procesados.add(txt_hash)
        
        # Análisis mejorado del mensaje
        res = analizar_mensaje(txt)
        if not res:
            motivos["incompleto"] += 1
            continue

        # Filtrar duplicados por URL
        url = res["url"]
        if url in urls_procesadas:
            motivos["duplicados"] += 1
            if DEBUG:
                logger.info(f"🔄 Duplicado por URL: {url}")
            continue
        urls_procesadas.add(url)

        modelo = res["modelo"]
        anio = res["año"]
        precio = res["precio"]
        roi = res["roi"]
        score = res["score"]
        relevante = res["relevante"]

        if DEBUG:
            logger.info(f"📅 Año detectado: {anio}")
            logger.info(f"💰 Precio detectado: Q{precio:,}")
            logger.info(f"📊 ROI calculado: {roi:.1f}%")
            logger.info(f"⭐ Score: {score}/10")

        # Validación adicional de coherencia precio-año
        if not validar_coherencia_precio_año(precio, anio):
            motivos["precio_año_incoherente"] += 1
            if DEBUG:
                logger.info(f"❌ Precio {precio} incoherente para año {anio}")
            continue

        # Filtrar contenido negativo y extranjero
        if es_extranjero(txt):
            motivos["extranjero"] += 1
            continue

        # Crear mensaje formateado
        mensaje = (
            f"🚘 *{modelo.title()}*\n"
            f"• Año: {anio}\n"
            f"• Precio: Q{precio:,}\n"
            f"• ROI: {roi:.1f}%\n"
            f"• Score: {score}/10\n"
            f"🔗 {url}"
        )

        # Clasificación mejorada
        if relevante:
            buenos.append(mensaje)
            resumen_relevantes.append((modelo, url, roi, score))
            if DEBUG:
                logger.info(f"✅ RELEVANTE: {modelo} - ROI {roi:.1f}%")
        elif score >= SCORE_MIN_DB and roi >= (ROI_MINIMO - 5):  # Potenciales con ROI ligeramente menor
            potenciales.append(mensaje)
            resumen_potenciales.append((modelo, url, roi, score))
            if DEBUG:
                logger.info(f"🟡 POTENCIAL: {modelo} - ROI {roi:.1f}%")
        else:
            # Categorizar rechazos para estadísticas
            if roi < ROI_MINIMO:
                motivos["roi_bajo"] += 1
            elif score < SCORE_MIN_DB:
                motivos["precio_fuera_rango"] += 1
            else:
                motivos["modelo_no_detectado"] += 1

        logger.info(
            f"🔍 {modelo} | Año {anio} | Precio Q{precio:,} | ROI {roi:.1f}% | Score {score}/10 | Relevante: {relevante}"
        )

    # Reportes detallados
    total_procesados = len(urls_procesadas)
    total_brutos = len(brutos)
    duplicados_totales = motivos["duplicados"] + (total_brutos - total_procesados)
    
    # 🛡️ Calcular tasa de relevancia de forma segura
    if total_procesados == 0:
        tasa_relevancia = "0.0"
    else:
        tasa_relevancia = f"{(len(buenos)/total_procesados*100):.1f}"
    
    reporte_inicial = (
        f"📊 *Resumen de procesamiento:*\n"
        f"• Anuncios encontrados: {total_brutos}\n"
        f"• Duplicados eliminados: {duplicados_totales}\n"
        f"• Procesados únicos: {total_procesados}\n"
        f"• Relevantes: {len(buenos)}\n"
        f"• Potenciales: {len(potenciales)}\n"
        f"• Tasa de relevancia: {tasa_relevancia}%"
    )
    
    await safe_send(reporte_inicial)


    # Reporte de motivos de descarte
    desc_total = sum(motivos.values())
    if desc_total > 0:
        motivos_texto = []
        for motivo, cantidad in motivos.items():
            if cantidad > 0:
                porcentaje = (cantidad / total_brutos) * 100
                motivos_texto.append(f"• {motivo.replace('_', ' ').title()}: {cantidad} ({porcentaje:.1f}%)")
        
        if motivos_texto:
            detalles = "\n".join(motivos_texto)
            await safe_send(f"📉 *Motivos de descarte:*\n{detalles}")

    # Enviar ofertas si las hay
    if not buenos and not potenciales:
        mensaje_vacio = f"📡 Ejecución a las {now_local.strftime('%H:%M')}"
        if now_local.hour == 18:  # Reporte diario
            stats = get_estadisticas_db()
            mensaje_vacio += f"\n📊 Base de datos: {stats['total_anuncios']} anuncios"
        await safe_send(mensaje_vacio + ", sin ofertas nuevas.")
        return

    # Enviar ofertas destacadas
    if buenos:
        for bloque in dividir_y_enviar("🎯 *Ofertas destacadas:*", buenos):
            await safe_send(bloque)

    # Enviar potenciales
    if potenciales:
        for bloque in dividir_y_enviar("🟡 *Potenciales (ROI≥5):*", potenciales):
            await safe_send(bloque)

    # Enviar pendientes manuales si los hay
    if pendientes:
        for bloque in dividir_y_enviar("📌 *Pendientes manuales:*", pendientes):
            await safe_send(bloque)

    # Estadísticas finales
    try:
        stats = get_estadisticas_db()
        reporte_final = (
            f"📊 *Estadísticas de base:*\n"
            f"• Total acumulado: {stats['total_anuncios']}\n"
            f"• Alta confianza: {stats['alta_confianza']}\n"
            f"• Baja confianza: {stats['baja_confianza']}\n"
            f"• Usando precios por defecto: {stats['porcentaje_defaults']}%"
        )
        await safe_send(reporte_final)
    except Exception as e:
        logger.error(f"❌ Error obteniendo estadísticas: {e}")

    # Log detallado para revisión manual
    logger.info("\n📋 Resumen final del scraping:")
    logger.info(f"Procesados: {total_procesados}")
    logger.info(f"Relevantes: {len(resumen_relevantes)}")
    logger.info(f"Potenciales: {len(resumen_potenciales)}")
    logger.info(f"Tasa de éxito: {((len(buenos) + len(potenciales))/total_procesados*100):.1f}%")

    if resumen_relevantes:
        logger.info("\n🟢 Relevantes encontrados:")
        for modelo, url, roi, score in resumen_relevantes:
            logger.info(f"• {modelo.title()} | ROI: {roi:.1f}% | Score: {score}/10")

    if resumen_potenciales:
        logger.info("\n🟡 Potenciales encontrados:")
        for modelo, url, roi, score in resumen_potenciales:
            logger.info(f"• {modelo.title()} | ROI: {roi:.1f}% | Score: {score}/10")

if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
