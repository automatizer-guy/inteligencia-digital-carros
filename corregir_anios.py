# corregir_anios.py
import os
import sqlite3
import asyncio
import logging
from datetime import datetime
from urllib.parse import urlparse

from playwright.async_api import async_playwright
from utils_analisis import extraer_anio

DB_PATH = os.environ.get("DB_PATH", "anuncios.db")
FB_COOKIES = os.environ.get("FB_COOKIES_JSON", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def limpiar_url(link: str) -> str:
    """
    Normaliza el path de Facebook Marketplace.
    """
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"


async def cargar_contexto_con_cookies(browser):
    """
    Carga cookies de FB para mantener sesión activa.
    """
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
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        )
    )
    await context.add_cookies(cookies)
    return context


async def procesar_anuncio(page, id_, link, anio_guardado, cursor):
    """
    Visita la URL, extrae texto, calcula año y actualiza si difiere.
    """
    url = limpiar_url(link)
    if not url:
        return

    try:
        # Carga la página y extrae texto
        await page.goto(url, timeout=20000)
        await page.wait_for_selector("div[role='main']", timeout=10000)
        texto = await page.inner_text("div[role='main']")
        if not texto or len(texto.strip()) < 50:
            texto = await page.title() or ""
    except Exception as e:
        logger.warning(f"⚠️ No pudo cargar {url}: {e}")
        return

    nuevo_anio = extraer_anio(texto)
    if not nuevo_anio:
        logger.info(f"– No se detectó año en anuncio {id_}.")
        return

    año_actual = datetime.now().year
    if not (1900 <= nuevo_anio <= año_actual):
        logger.info(f"– Año {nuevo_anio} fuera de rango en anuncio {id_}.")
        return

    if nuevo_anio != anio_guardado:
        cursor.execute(
            "UPDATE anuncios SET anio = ? WHERE id = ?",
            (nuevo_anio, id_)
        )
        logger.info(f"✅ Anuncio {id_}: {anio_guardado} → {nuevo_anio}")


async def main():
    # Conexión a DB
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id, link, anio FROM anuncios")
    registros = cursor.fetchall()
    total = len(registros)
    logger.info(f"Iniciando revisión de {total} anuncios…")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await cargar_contexto_con_cookies(browser)
        page = await context.new_page()

        for idx, (id_, link, anio) in enumerate(registros, start=1):
            logger.info(f"[{idx}/{total}] Procesando ID={id_}")
            await procesar_anuncio(page, id_, link, anio, cursor)

        await browser.close()

    conn.commit()
    conn.close()
    logger.info("Corrección completada y base de datos actualizada.")


if __name__ == "__main__":
    asyncio.run(main())
