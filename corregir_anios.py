import os
import sqlite3
import asyncio
import logging
import json
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from playwright.async_api import async_playwright
from utils_analisis import extraer_anio

DB_PATH = os.environ.get("DB_PATH", "anuncios.db")
FB_COOKIES = os.environ.get("FB_COOKIES_JSON", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 50  # eficiencia: procesa anuncios en bloques

def limpiar_url(link: str) -> str:
    if not link:
        return ""
    parsed = urlparse(link.strip())
    return urlunparse(("https", "www.facebook.com", parsed.path.rstrip("/"), "", parsed.query, ""))


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
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
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
    except Exception as e:
        logger.warning(f"⚠️ Error al acceder {url}: {e}")
        return

    # Detectar redirecciones o expirado
    if "/marketplace/item/" not in page.url:
        logger.info(f"→ Redirigido o vencido: {link}")
        return

    # Extraer texto usando varios selectores para compatibilidad
    texto = ""
    selectors = ["div[role='main']", "[data-pagelet='Marketplace']", "body"]
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=10000)
            texto = await page.inner_text(sel)
            if texto and len(texto.strip()) > 50:
                break
        except:
            continue

    if not texto or any(m in texto for m in ["no hay productos", "no disponible", "no encontrado"]):
        logger.info(f"→ Página vacía o genérica: {link}")
        return

    nuevo_anio = extraer_anio(texto, modelo, precio)
    año_actual = datetime.now().year

    # Logging más explícito para casos no actualizados
    if not nuevo_anio:
        logger.info(f"↪️ No se detectó año en {link} | Guardado: {anio_guardado}")
        return

    if nuevo_anio == anio_guardado:
        logger.info(f"↪️ Año sin cambios: {anio_guardado} en {link}")
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
    logger.info(f"📦 Procesando {total} anuncios en bloques de {BATCH_SIZE}…")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await cargar_contexto_con_cookies(browser)
        page = await context.new_page()

        for start in range(0, total, BATCH_SIZE):
            batch = registros[start:start + BATCH_SIZE]
            for idx, (link, modelo, anio, precio) in enumerate(batch, start=start + 1):
                logger.info(f"[{idx}/{total}] {modelo} | Año en BD: {anio} → {link}")
                await procesar_anuncio(page, link, modelo, anio, precio, cursor)
            await asyncio.sleep(1)  # pequeña pausa entre bloques

        await browser.close()

    conn.commit()
    conn.close()
    logger.info("✅ Corrección finalizada.")

if __name__ == "__main__":
    asyncio.run(main())
