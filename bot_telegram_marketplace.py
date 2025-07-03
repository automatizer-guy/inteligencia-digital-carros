import asyncio
import re
import os
import sqlite3
import unicodedata
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

# 🌱 Inicializar tabla si no exista
inicializar_tabla_anuncios()

# 🔐 Leer variables desde entorno, eliminando espacios y saltos de línea
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["CHAT_ID"].strip())

bot = Bot(token=BOT_TOKEN)

# 🛣️ Ruta base de la base de datos
DB_PATH = os.environ.get("DB_PATH", "upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# 📨 Envío seguro de texto plano
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

# 📨 Envío seguro con botón
async def safe_send_with_button(text: str, url: str):
    url = limpiar_link(url)
    print(f"🔗 Enviando botón con URL: {repr(url)}")
    escaped = escape_markdown(text.strip(), version=2)
    button = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Ver anuncio", url=url)]
    ])
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

# 📦 Validación de URL
def link_valido(url: str) -> bool:
    return bool(url and url.startswith("https://") and '\n' not in url and '\r' not in url)

# 📦 Extraer datos útiles del mensaje
def extraer_info(txt: str):
    link_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", txt)
    link_url = limpiar_link(link_match.group(0)) if link_match else ""

    año_match = re.search(r"Año: (\d{4})", txt)
    precio_match = re.search(r"Precio: Q([\d,]+)", txt)
    modelo_match = re.search(r"🚘 \*(.+?)\*", txt)

    año = int(año_match.group(1)) if año_match else None
    precio = int(precio_match.group(1).replace(",", "")) if precio_match else None
    modelo_txt = modelo_match.group(1).lower() if modelo_match else ""

    return link_url, año, precio, modelo_txt

# 🧪 Extraer score
def extraer_score(texto: str) -> int:
    match = re.search(r"Score:\s?(\d+)/10", texto)
    return int(match.group(1)) if match else 0

# ✅ Validar mensaje y ROI, devuelve modelo_detectado
def mensaje_valido(txt: str):
    link, año, precio, modelo_txt = extraer_info(txt)
    if not all([link, año, precio, modelo_txt]):
        print(f"🚫 Datos incompletos → {repr((link, año, precio, modelo_txt))}")
        return False, 0.0, None

    modelos_conocidos = [
        "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
        "kia picanto", "chevrolet spark", "nissan march", "suzuki alto",
        "suzuki swift", "suzuki grand vitara", "hyundai accent", "hyundai i10",
        "kia rio", "mitsubishi mirage", "toyota", "honda"
    ]
    modelo_detectado = next((m for m in modelos_conocidos if coincide_modelo(modelo_txt, m)), None)
    if not modelo_detectado:
        print(f"❓ Modelo no detectado: {modelo_txt}")
        return False, 0.0, None

    roi = calcular_roi_real(modelo_detectado, precio, año)
    return roi >= 10, roi, modelo_detectado

# 🧠 Función principal para enviar ofertas
async def enviar_ofertas():
    print("📡 Buscando autos...")
    brutos, pendientes = await buscar_autos_marketplace()

    buenos, potenciales, descartados = [], [], []
    for txt in brutos:
        txt = txt.strip()
        valido, roi, modelo = mensaje_valido(txt)
        score = extraer_score(txt)
        print(f"🔎 ROI: {roi:.1f}% | Score: {score} | Modelo: {modelo}")

        if valido and score >= 6:
            buenos.append(txt)
        elif roi >= 7 and score >= 4:
            potenciales.append(txt)
        else:
            descartados.append(txt)

    total = len(buenos) + len(potenciales) + len(descartados)
    print(f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}")
    await safe_send(f"📊 Procesados: {total} | Relevantes: {len(buenos)} | Potenciales: {len(potenciales)}")

    if not buenos and not potenciales:
        # Solo enviar una vez al final del día (18:00 hora local Guatemala)
        hora_local = datetime.now(ZoneInfo("America/Guatemala")).hour
        FINAL_HOUR = 18
        if hora_local == FINAL_HOUR:
            hora_str = datetime.now(ZoneInfo("America/Guatemala")).strftime("%H:%M")
            await safe_send(f"📡 Bot ejecutado a las {hora_str}, sin ofertas en todo el día.")
        return

    # 🚘 Enviar anuncios relevantes
    for b in buenos:
        link_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", b)
        link_url = limpiar_link(link_match.group(0)) if link_match else None
        texto_sin_link = re.sub(r"\n?🔗 https://www\.facebook\.com/marketplace/item/\d+", "", b).strip()
        texto_sin_link = ''.join(c for c in texto_sin_link if c.isprintable())

        if not link_valido(link_url):
            print(f"🧨 Link inválido o sucio → {repr(link_url)}")
            await safe_send(b)
        else:
            print(f"📤 Enviando con botón → {link_url}")
            await safe_send_with_button(texto_sin_link, link_url)
        await asyncio.sleep(1)

    # 📌 Mostrar pendientes manuales
    if pendientes:
        pm = "📌 *Pendientes de revisión manual:*\n\n" + "\n\n".join(p.strip() for p in pendientes)
        for i in range(0, len(pm), 3000):
            await safe_send(pm[i:i+3000])
            await asyncio.sleep(1)

    # 📦 Mostrar total acumulado en base de datos
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total_db = cur.fetchone()[0]
    print(f"📦 Total acumulado en base: {total_db}")
    await safe_send(f"📦 Total acumulado en base: {total_db} anuncios")

# 🚀 Ejecutar
if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
