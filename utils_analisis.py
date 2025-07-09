import asyncio
import os
import re
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Bot
from telegram.helpers import escape_markdown
from scraper_marketplace import buscar_autos_marketplace
from utils_analisis import (
    inicializar_tabla_anuncios,
    analizar_mensaje,
    extraer_score,
    limpiar_link,
    SCORE_MIN_DB,
    SCORE_MIN_TELEGRAM,
    ROI_MINIMO,
    modelos_bajo_rendimiento,
    MODELOS_INTERES,
)

# 🌱 Inicializar base de datos
inicializar_tabla_anuncios()

# 🔐 Variables desde entorno
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["CHAT_ID"].strip())
DB_PATH = os.environ.get("DB_PATH", "upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

bot = Bot(token=BOT_TOKEN)

# 📨 Envío seguro
async def safe_send(text: str, parse_mode="MarkdownV2"):
    escaped = escape_markdown(text.strip(), version=2)
    for _ in range(3):
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=escaped,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"⚠️ Error al enviar mensaje: {e}")
            await asyncio.sleep(1)

# 🧪 Envío principal
async def enviar_ofertas():
    print("📡 Buscando autos...")

    # 📉 Detectar modelos de bajo rendimiento y omitirlos
    bajos = modelos_bajo_rendimiento()
    activos = [m for m in MODELOS_INTERES if m not in bajos]
    print(f"✅ Modelos activos: {activos}")

    # 🔍 Buscar anuncios
    brutos, pendientes = await buscar_autos_marketplace(modelos_override=activos)

    buenos, potenciales = [], []
    descartados = {"incompleto": 0, "extranjero": 0, "modelo no detectado": 0,
                   "año fuera de rango": 0, "precio fuera de rango": 0, "roi bajo": 0}
    total = len(brutos)

    for txt in brutos:
        txt = txt.strip()
        valido, roi, modelo, motivo = analizar_mensaje(txt)
        score = extraer_score(txt)
        print(f"🔎 ROI: {roi:.1f}% | Score: {score} | Modelo: {modelo} | Motivo: {motivo}")

        if valido and score >= SCORE_MIN_TELEGRAM:
            buenos.append(txt)
        elif roi >= ROI_MINIMO and score >= SCORE_MIN_DB:
            potenciales.append(txt)
        else:
            if motivo in descartados:
                descartados[motivo] += 1

    # 🪄 Formato unificado
    def unir_mensajes(lista: list) -> str:
        return "\n\n".join(m.strip() for m in lista)

    resumen = f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}"
    await safe_send(resumen)

    if total > 0 and sum(descartados.values()) > 0:
        errores = "\n".join([f"• {motivo}: {cant}" for motivo, cant in descartados.items() if cant])
        await safe_send(f"📉 Anuncios descartados:\n{errores}")

    # 🔇 Si no hay mensajes buenos ni potenciales
    if not buenos and not potenciales:
        if datetime.now(ZoneInfo("America/Guatemala")).hour == 18:
            hora_str = datetime.now(ZoneInfo("America/Guatemala")).strftime("%H:%M")
            await safe_send(f"📡 Bot ejecutado a las {hora_str}, sin ofertas nuevas.")
        return

    # 📨 Unificar y enviar mensajes relevantes
    if buenos:
        mensajes_unificados = unir_mensajes(buenos)
        await safe_send(mensajes_unificados)

    # 📎 Mostrar pendientes
    if pendientes:
        texto = "📌 *Pendientes de revisión manual:*\n\n" + "\n\n".join(p.strip() for p in pendientes)
        for i in range(0, len(texto), 3000):
            await safe_send(texto[i:i+3000])
            await asyncio.sleep(1)

    # 📦 Mostrar acumulado
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total_db = cur.fetchone()[0]
    await safe_send(f"📦 Total acumulado en base: {total_db} anuncios")

# 🚀 Lanzar
if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
