import os
import sqlite3
import asyncio
import logging
import json
from datetime import datetime
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from utils_analisis import extraer_anio

DB_PATH = os.environ.get("DB_PATH", "anuncios.db")
FB_COOKIES = os.environ.get("FB_COOKIES_JSON", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"


async def cargar_contexto_con_cookies(browser):
    if not FB_COOKIES:
        logger.warning("⚠️ FB_COOKIES_JSON no definido; usando sesión anónima.")
        return await browser.new_context(locale="es-ES")

    try:
        cookies = json.loads(FB_COOKIES)
    except Exception as e:
        logger.error(f"❌ Error parseando cookies: {e}")
        return await browser.new_context(locale="es-ES")

    context = await browser.new_context(
        locale="es-ES",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )
    await context.add_cookies(cookies)
    return context


async def procesar_anuncio(page, link, modelo, anio_guardado, precio, cursor):
    url = limpiar_url(link)
    if not url or "/marketplace/item/" not in url:
        logger.info(f"→ Enlace no válido de ítem: {link}")
        return

    try:
        await page.goto(url, timeout=20000)
        await page.wait_for_selector("div[role='main']", timeout=10000)
        texto = await page.inner_text("div[role='main']")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo cargar {url}: {e}")
        return

    # Validar si la página fue redirigida o está vacía
    if "/marketplace/item/" not in page.url or any(
        frase in texto for frase in [
            "Actualmente no hay productos en tu zona",
            "Anuncio no disponible",
            "Contenido no encontrado"
        ]
    ):
        logger.info(f"→ Anuncio vencido o inaccesible: {link}")
        return

    nuevo_anio = extraer_anio(texto, modelo, precio)
    año_actual = datetime.now().year

    # Mostrar log incluso si no se actualiza
    if not nuevo_anio or nuevo_anio == anio_guardado:
        logger.info(f"↪️ Año sin cambio: {anio_guardado} en {link}")
        return

    if not (1980 <= nuevo_anio <= año_actual + 2):
        logger.info(f"– Año fuera de rango: {nuevo_anio} en {link}")
        return

    cursor.execute("UPDATE anuncios SET anio = ? WHERE link = ?", (nuevo_anio, link))
    logger.info(f"✅ Año corregido en {link}: {anio_guardado} → {nuevo_anio}")



async def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT link, modelo, anio, precio FROM anuncios")
    registros = cursor.fetchall()
    total = len(registros)
    logger.info(f"📦 Revisando {total} anuncios…")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await cargar_contexto_con_cookies(browser)
        page = await context.new_page()

        for idx, (link, modelo, anio, precio) in enumerate(registros, start=1):
            logger.info(f"[{idx}/{total}] Procesando: {modelo} | {link}")
            await procesar_anuncio(page, link, modelo, anio, precio, cursor)

        await browser.close()

    conn.commit()
    conn.close()
    logger.info("✅ Corrección finalizada.")

if __name__ == "__main__":
    asyncio.run(main())
