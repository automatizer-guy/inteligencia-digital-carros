import asyncio
import os
import sqlite3
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Bot
from telegram.helpers import escape_markdown
from scraper_marketplace import buscar_autos_marketplace
from utils_analisis import (
    inicializar_tabla_anuncios,
    analizar_mensaje,
    limpiar_link,
    es_extranjero,
    SCORE_MIN_DB,
    SCORE_MIN_TELEGRAM,
    ROI_MINIMO,
    modelos_bajo_rendimiento,
    MODELOS_INTERES,
)

# —— Logger configurado —— 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# —— Inicializar DB —— 
inicializar_tabla_anuncios()

# —— Variables de entorno —— 
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
CHAT_ID   = int(os.environ["CHAT_ID"].strip())
DB_PATH   = os.environ.get("DB_PATH", "upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

bot = Bot(token=BOT_TOKEN)

# —— Envío seguro a Telegram —— 
async def safe_send(text: str, parse_mode="MarkdownV2"):
    md = escape_markdown(text, version=2)
    for _ in range(3):
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=md,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Error enviando a Telegram (reintento): {e}")
            await asyncio.sleep(1)

# —— Lógica principal —— 
async def enviar_ofertas():
    logger.info("📡 Iniciando bot de Telegram")
    now_local = datetime.now(ZoneInfo("America/Guatemala"))

    # 1) Omitir modelos de bajo rendimiento
    bajos   = modelos_bajo_rendimiento()
    activos = [m for m in MODELOS_INTERES if m not in bajos]
    logger.info(f"✅ Modelos activos: {activos}")

    # 2) Llamar al scraper (con manejo de errores)
    try:
        brutos, pendientes = await buscar_autos_marketplace(modelos_override=activos)
    except Exception as e:
        logger.error(f"❌ Error en scraper: {e}")
        await safe_send("❌ Error ejecutando scraper, revisa logs.")
        return

    # 3) Clasificar resultados
    buenos, potenciales = [], []
    motivos = {
        "incompleto": 0,
        "extranjero": 0,
        "modelo no detectado": 0,
        "año fuera de rango": 0,
        "precio fuera de rango": 0,
        "roi bajo": 0
    }

    for txt in brutos:
        res = analizar_mensaje(txt)
        if not res:
            motivos["incompleto"] += 1
            continue

        url, modelo, anio, precio, roi, score, relevante = (
            res["url"], res["modelo"], res["año"],
            res["precio"], res["roi"], res["score"], res["relevante"]
        )

        # determinar motivo si no relevante
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

        # asignar a listas
        if relevante and score >= SCORE_MIN_TELEGRAM:
            buenos.append(txt)
        elif score >= SCORE_MIN_DB and roi >= ROI_MINIMO:
            potenciales.append(txt)

        logger.info(
            f"🔎 {modelo} | Año {anio} | Precio {precio} | ROI {roi:.1f}% "
            f"| Score {score}/10 | Relevante: {relevante}"
        )

    total = len(brutos)
    # 4) Enviar resumen general
    resumen = f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}"
    await safe_send(resumen)

    # 5) Enviar detalles de descartes
    desc_total = sum(motivos.values())
    if desc_total:
        detalles = "\n".join(f"• {k}: {v}" for k, v in motivos.items() if v)
        await safe_send(f"📉 Descartados:\n{detalles}")

    # 6) Si no hay hits, solo a las 18:00
    if not buenos and not potenciales:
        if now_local.hour == 18:
            await safe_send(f"📡 Ejecución a las {now_local.strftime('%H:%M')}, sin ofertas.")
        return

    # 7) Enviar relevantes
    if buenos:
        texto = "\n\n".join(buenos)
        await safe_send(texto)

    # 8) Enviar potenciales separados
    if potenciales:
        texto = "🟡 Potenciales (score>=4&roi>=10):\n" + "\n\n".join(potenciales)
        await safe_send(texto)

    # 9) Pendientes manuales
    if pendientes:
        texto = "📌 Pendientes manuales:\n" + "\n\n".join(pendientes)
        # fragmentar en trozos de 3000 chars
        for i in range(0, len(texto), 3000):
            await safe_send(texto[i:i+3000])

    # 10) Total acumulado en BD
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total_db = cur.fetchone()[0]
    await safe_send(f"📦 Total en base: {total_db} anuncios")

# —— Entrypoint —— 
if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
