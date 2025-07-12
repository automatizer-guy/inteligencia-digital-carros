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
    modelos_bajo_rendimiento, MODELOS_INTERES, escapar_multilinea
)

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
inicializar_tabla_anuncios()

# Configuración del bot
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
        "incompleto": 0, "extranjero": 0, "modelo no detectado": 0,
        "año fuera de rango": 0, "precio fuera de rango": 0, "roi bajo": 0
    }

    for txt in brutos:
        res = analizar_mensaje(txt)
        if not res:
            motivos["incompleto"] += 1
            continue

        url, modelo, anio, precio, roi, score, relevante = (
            res["url"], res["modelo"], res["año"], res["precio"],
            res["roi"], res["score"], res["relevante"]
        )

        mensaje = (
            f"🚘 *{modelo.title()}*\n"
            f"• Año: {anio}\n"
            f"• Precio: Q{precio:,}\n"
            f"• ROI: {roi:.1f}%\n"
            f"• Score: {score}/10\n"
            f"🔗 {url}"
        )

        motivo = None
        if not relevante:
            if es_extranjero(txt):
                motivo = "extranjero"
            elif roi < ROI_MINIMO:
                motivo = "roi bajo"
            elif score < SCORE_MIN_DB:
                motivo = "precio fuera de rango"
            else:
                motivo = "modelo no detectado"
            motivos[motivo] = motivos.get(motivo, 0) + 1

        if relevante and score >= SCORE_MIN_TELEGRAM:
            buenos.append(mensaje)
            resumen_relevantes.append((modelo, url, roi, score))
        elif score >= SCORE_MIN_DB and roi >= ROI_MINIMO:
            potenciales.append(mensaje)
            resumen_potenciales.append((modelo, url, roi, score))

        logger.info(
            f"🔍 {modelo} | Año {anio} | Precio {precio} | ROI {roi:.1f}% | Score {score}/10 | Relevante: {relevante}"
        )

    total = len(brutos)
    resumen_txt = f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}"
    await safe_send(resumen_txt)

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

    for bloque in dividir_y_enviar("🟡 *Potenciales (score≥4 & roi≥10):*", potenciales):
        await safe_send(bloque)

    for bloque in dividir_y_enviar("📌 *Pendientes manuales:*", pendientes):
        await safe_send(bloque)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total_db = cur.fetchone()[0]
    await safe_send(f"📦 Total acumulado en base: {total_db} anuncios")

    # 🧾 Auditoría en GitHub Logs
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
