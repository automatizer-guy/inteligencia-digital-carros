import asyncio
import re
import os
import sqlite3
import unicodedata
from datetime import datetime
from scraper_marketplace import buscar_autos_marketplace
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.helpers import escape_markdown
from utils_analisis import inicializar_tabla_anuncios, calcular_roi_real, coincide_modelo, limpiar_link

# 🌱 Inicializar tabla si no existe
inicializar_tabla_anuncios()

# 🔐 Leer variables desde entorno
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ["CHAT_ID"])

bot = Bot(token=BOT_TOKEN)

# 🛣️ Ruta base de la base de datos
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
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

# 📨 Envío seguro con botón (corregido)
async def safe_send_with_button(text: str, url: str):
    url = limpiar_link(url)  # ✅ Limpieza definitiva de URL
    print(f"🔗 Enviando botón con URL: {repr(url)}")  # Para depuración
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

# 📦 Extraer datos útiles del mensaje para validar
def extraer_info(txt: str):
    link_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", txt)
    link_url = limpiar_link(link_match.group(0)) if link_match else ""

    año = re.search(r"Año: (\d{4})", txt)
    precio = re.search(r"Precio: Q([\d,]+)", txt)
    modelo = re.search(r"🚘 \*(.+?)\*", txt)

    return (
        link_url,
        int(año.group(1)) if año else None,
        int(precio.group(1).replace(",", "")) if precio else None,
        modelo.group(1).lower() if modelo else ""
    )

# ✅ Verifica si el mensaje contiene información válida y calcula ROI
def mensaje_valido(txt: str):
    link, año, precio, modelo_txt = extraer_info(txt)
    if not all([link, año, precio, modelo_txt]):
        print(f"🚫 Datos incompletos → {repr((link, año, precio, modelo_txt))}")
        return False, 0.0

    modelo_detectado = next((m for m in [
        "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
        "kia picanto", "chevrolet spark", "nissan march", "suzuki alto",
        "suzuki swift", "suzuki grand vitara", "hyundai accent", "hyundai i10",
        "kia rio", "mitsubishi mirage", "toyota", "honda"
    ] if coincide_modelo(modelo_txt, m)), None)

    if not modelo_detectado:
        print(f"❓ Modelo no detectado: {modelo_txt}")
        return False, 0.0

    roi = calcular_roi_real(modelo_detectado, precio, año)
    return roi >= 10, roi

# 🧠 Función principal
async def enviar_ofertas():
    print("📡 Buscando autos...")
    brutos, pendientes = await buscar_autos_marketplace()

    buenos = []
    potenciales = []
    descartados = []

    for txt in brutos:
        txt = txt.strip()
        valido, roi = mensaje_valido(txt)
        score_match = re.search(r"Score:\s?(\d+)/10", txt)
        score = int(score_match.group(1)) if score_match else 0

        print(f"🔎 ROI: {roi:.1f}% | Score: {score}")

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
        hora = datetime.now().strftime("%H:%M")
        await safe_send(f"📡 Bot ejecutado a las {hora}, sin ofertas nuevas.")
        return

    # 🚘 Enviar buenos resultados
    for b in buenos:
        link_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", b)
        link_url = limpiar_link(link_match.group(0)) if link_match else None
        texto_sin_link = re.sub(r"\n?🔗 https://www\.facebook\.com/marketplace/item/\d+", "", b).strip()
        texto_sin_link = ''.join(c for c in texto_sin_link if c.isprintable())

        if not link_url or '\n' in link_url or '\r' in link_url or not link_url.startswith("https://"):
            print(f"🧨 Link inválido o sucio → {repr(link_url)}")
            await safe_send(b)
        else:
            print(f"📤 Enviando con botón → {link_url}")
            await safe_send_with_button(texto_sin_link, link_url)
        await asyncio.sleep(1)

    # 📌 Mostrar pendientes si existen
    if pendientes:
        pm = "📌 *Pendientes de revisión manual:*\n\n" + "\n\n".join(p.strip() for p in pendientes)
        for i in range(0, len(pm), 3000):
            await safe_send(pm[i:i+3000])
            await asyncio.sleep(1)

    # 📦 Mostrar cantidad acumulada
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM anuncios")
    total = cur.fetchone()[0]
    conn.close()

    print(f"📦 Total acumulado en base: {total}")
    await safe_send(f"📦 Total acumulado en base: {total} anuncios")

# 🚀 Ejecutar
if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
