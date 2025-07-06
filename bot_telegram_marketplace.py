import asyncio
import os
import re
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from scraper_marketplace import buscar_autos_marketplace
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.helpers import escape_markdown
from utils_analisis import (
    inicializar_tabla_anuncios,
    calcular_roi_real,
    coincide_modelo,
    limpiar_link
)

# 🌱 Inicializar base de datos
inicializar_tabla_anuncios()

# 🔐 Variables desde entorno
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["CHAT_ID"].strip())
DB_PATH = os.environ.get("DB_PATH", "upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

bot = Bot(token=BOT_TOKEN)

# 📨 Envío seguro sin botón
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

# 📨 Envío con botón
async def safe_send_with_button(text: str, url: str):
    url = limpiar_link(url)
    print(f"🔗 Enviando botón con URL: {repr(url)}")
    escaped = escape_markdown(text.strip(), version=2)
    button = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Ver anuncio", url=url)]])
    for _ in range(3):
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=escaped,
                parse_mode="MarkdownV2",
                reply_markup=button,
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"⚠️ Error al enviar mensaje con botón: {e}")
            await asyncio.sleep(1)

# 🧪 Utilidades de análisis
def link_valido(url: str) -> bool:
    return bool(url and url.startswith("https://") and '\n' not in url and '\r' not in url)

def extraer_info(txt: str):
    url_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", txt)
    anio_match = re.search(r"Año: (\d{4})", txt)
    precio_match = re.search(r"Precio: Q([\d,]+)", txt)
    modelo_match = re.search(r"🚘 \*(.+?)\*", txt)

    url = limpiar_link(url_match.group()) if url_match else ""
    anio = int(anio_match.group(1)) if anio_match else None
    precio = int(precio_match.group(1).replace(",", "")) if precio_match else None
    modelo_txt = modelo_match.group(1).lower() if modelo_match else ""

    return url, anio, precio, modelo_txt

def extraer_score(txt: str) -> int:
    match = re.search(r"Score:\s?(\d+)/10", txt)
    return int(match.group(1)) if match else 0

def mensaje_valido(txt: str):
    url, anio, precio, modelo_txt = extraer_info(txt)
    if not all([url, anio, precio, modelo_txt]):
        print(f"🚫 Incompleto → {repr((url, anio, precio, modelo_txt))}")
        return False, 0.0, None

    modelos = [
        "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
        "kia picanto", "chevrolet spark", "nissan march", "suzuki alto",
        "suzuki swift", "suzuki grand vitara", "hyundai accent", "hyundai i10",
        "kia rio", "mitsubishi mirage", "toyota", "honda"
    ]
    detectado = next((m for m in modelos if coincide_modelo(modelo_txt, m)), None)
    if not detectado:
        print(f"❓ Modelo no detectado: {modelo_txt}")
        return False, 0.0, None

    roi = calcular_roi_real(detectado, precio, anio)
    return roi >= 10, roi, detectado

# 🚘 Envío principal
async def enviar_ofertas():
    print("📡 Buscando autos...")
    brutos, pendientes = await buscar_autos_marketplace()

    buenos, potenciales = [], []
    for txt in brutos:
        txt = txt.strip()
        valido, roi, modelo = mensaje_valido(txt)
        score = extraer_score(txt)
        print(f"🔎 ROI: {roi:.1f}% | Score: {score} | Modelo: {modelo}")

        if valido and score >= 6:
            buenos.append(txt)
        elif roi >= 7 and score >= 4:
            potenciales.append(txt)

    # 🪄 Formato unificado por bloques
    def unir_mensajes(lista: list) -> str:
        return "\n\n".join(m.strip() for m in lista)

    total = len(brutos)
    resumen = f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}"
    await safe_send(resumen)

    # 🔇 Si no hay mensajes relevantes, solo resumen
    if not buenos and not potenciales:
        if datetime.now(ZoneInfo("America/Guatemala")).hour == 18:
            hora_str = datetime.now(ZoneInfo("America/Guatemala")).strftime("%H:%M")
            await safe_send(f"📡 Bot ejecutado a las {hora_str}, sin ofertas nuevas.")
        return

    # 📨 Enviar todos los relevantes como un solo mensaje
    if buenos:
        mensajes_unificados = unir_mensajes(buenos)
        await safe_send(mensajes_unificados)

    # 📎 Pendientes de revisión
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
