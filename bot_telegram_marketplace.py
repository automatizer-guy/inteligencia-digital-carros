import asyncio
import re
import os
import sqlite3
from datetime import datetime
from scraper_marketplace import buscar_autos_marketplace
from telegram import Bot
from telegram.helpers import escape_markdown
from utils_analisis import inicializar_tabla_anuncios, calcular_roi_real, coincide_modelo

# 🌱 Inicializar tabla si no existe
inicializar_tabla_anuncios()

# 🔐 Leer variables desde entorno
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ["CHAT_ID"])

bot = Bot(token=BOT_TOKEN)

# 🛣️ Ruta base
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

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

def extraer_info(txt: str):
    link_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", txt)
    link_url = link_match.group(0).strip().replace('\n', '').replace('\r', '').replace(' ', '') if link_match else ""

    año = re.search(r"Año: (\d{4})", txt)
    precio = re.search(r"Precio: Q([\d,]+)", txt)
    modelo = re.search(r"🚘 \*(.+?)\*", txt)

    return (
        link_url,
        int(año.group(1)) if año else None,
        int(precio.group(1).replace(",", "")) if precio else None,
        modelo.group(1).lower() if modelo else ""
    )

def mensaje_valido(txt: str):
    link, año, precio, modelo_txt = extraer_info(txt)
    if not all([link, año, precio, modelo_txt]):
        print(f"🚫 Datos incompletos → {repr((link, año, precio, modelo_txt))}")
        return False, 0.0

    modelo_detectado = next((
        m for m in [
            "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
            "kia picanto", "chevrolet spark", "nissan march", "suzuki alto",
            "suzuki swift", "suzuki grand vitara", "hyundai accent", "hyundai i10",
            "kia rio", "mitsubishi mirage", "toyota", "honda"
        ] if coincide_modelo(modelo_txt, m)
    ), None)

    if not modelo_detectado:
        print(f"❓ Modelo no detectado: {modelo_txt}")
        return False, 0.0

    roi = calcular_roi_real(modelo_detectado, precio, año)
    return roi >= 10, roi

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

    buenos.sort(key=lambda x: float(re.search(r"ROI:\s?([\d\.-]+)%", x).group(1)), reverse=True)
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    texto = f"🚘 *Ofertas nuevas ({fecha}):*\n\n" + "\n\n".join(b.strip() for b in buenos)
    partes = [texto[i:i+3000] for i in range(0, len(texto), 3000)]

    if not partes:
        print("❌ No se generó texto para enviar a Telegram.")
    for p in partes:
        print("📤 Enviando mensaje a Telegram:\n", p[:200].replace("\n", " "))
        await safe_send(p)
        await asyncio.sleep(1)

    if pendientes:
        pm = "📌 *Pendientes de revisión manual:*\n\n" + "\n\n".join(p.strip() for p in pendientes)
        for i in range(0, len(pm), 3000):
            await safe_send(pm[i:i+3000])
            await asyncio.sleep(1)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM anuncios")
    total = cur.fetchone()[0]
    conn.close()

    print(f"📦 Total acumulado en base: {total}")
    await safe_send(f"📦 Total acumulado en base: {total} anuncios")

if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
