import asyncio
import re
from datetime import datetime
from scraper_marketplace import buscar_autos_marketplace
from telegram import Bot
from telegram.helpers import escape_markdown
import sqlite3
import os
from utils_analisis import inicializar_tabla_anuncios

# 🌱 Inicializar tabla si no existe
inicializar_tabla_anuncios()

# 🔐 Leer variables desde entorno (GitHub Actions o local)
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = int(os.environ["CHAT_ID"])

bot = Bot(token=BOT_TOKEN)

# 🛣️ Ruta a la base central
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# 🔍 ROI real desde la base
def get_precio_minimo(modelo: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(precio) FROM anuncios WHERE modelo = ?
    """, (modelo,))
    resultado = cur.fetchone()
    conn.close()
    return resultado[0] if resultado and resultado[0] else 0

def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    precio_obj = get_precio_minimo(modelo)
    if not precio_obj or precio_compra <= 0:
        return 0.0

    antiguedad = max(0, datetime.now().year - año)
    penal = max(0, antiguedad - 10) * 0.02
    precio_dep = precio_obj * (1 - penal)

    inversion = precio_compra + costo_extra
    ganancia = precio_dep - inversion
    roi = (ganancia / inversion) * 100 if inversion > 0 else 0.0
    return round(roi, 1)

def ajustar_roi(texto: str) -> str:
    modelo_match = re.search(r"• Año: \d+\n• Precio: Q([\d,]+)", texto)
    anio_match   = re.search(r"• Año: (\d+)", texto)
    link_match   = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", texto)
    modelo_txt   = texto.split("🚘 *")[-1].split("*")[0].lower()

    if not (modelo_match and anio_match and link_match):
        return texto

    precio = int(modelo_match.group(1).replace(",", ""))
    anio = int(anio_match.group(1))

    posibles = [
        "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
        "kia picanto", "chevrolet spark", "honda", "nissan march",
        "suzuki alto", "suzuki swift", "suzuki grand vitara",
        "hyundai accent", "hyundai i10", "kia rio"
    ]
    modelo = next((m for m in posibles if m in modelo_txt), None)
    if not modelo:
        return texto

    roi = calcular_roi_real(modelo, precio, anio)
    texto = re.sub(r"ROI: [\d\.-]+%", f"ROI: {roi}%", texto)
    return texto

async def safe_send(text: str, parse_mode="MarkdownV2"):
    escaped = escape_markdown(text, version=2)
    for _ in range(3):
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=escaped,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
        except Exception:
            await asyncio.sleep(1)

def extraer_roi(txt: str) -> float:
    m = re.search(r"ROI:\s?([\d\.-]+)%", txt)
    return float(m.group(1)) if m else 0.0

def extraer_score(txt: str) -> int:
    m = re.search(r"Score:\s?(\d+)/10", txt)
    return int(m.group(1)) if m else 0

async def enviar_ofertas():
    print("📡 Buscando autos...")
    brutos, pendientes = await buscar_autos_marketplace()

    ajustados = [ajustar_roi(txt) for txt in brutos]
    buenos = [r for r in ajustados if extraer_roi(r) >= 10 and extraer_score(r) >= 6]

    if not buenos:
        print("📭 No hay ofertas relevantes.")
        return

    buenos.sort(key=extraer_roi, reverse=True)
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M")
    partes = []
    texto = f"🚘 *Ofertas nuevas ({fecha}):*\n\n" + "\n\n".join(buenos)
    for i in range(0, len(texto), 3000):
        partes.append(texto[i:i+3000])

    for p in partes:
        await safe_send(p)
        await asyncio.sleep(1)

    if pendientes:
        pm = "📌 *Pendientes de revisión manual:*\n\n" + "\n\n".join(pendientes)
        for i in range(0, len(pm), 3000):
            await safe_send(pm[i:i+3000])
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(enviar_ofertas())
