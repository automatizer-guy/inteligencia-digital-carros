import re
import os
import json
import random
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo,
    existe_en_db, insertar_anuncio_db, inicializar_tabla_anuncios, limpiar_link
)

# Configuración Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constantes configurables
MODELOS_INTERES = [
    "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
    "kia picanto", "chevrolet spark", "honda", "nissan march",
    "suzuki alto", "suzuki swift", "suzuki grand vitara",
    "hyundai accent", "hyundai i10", "kia rio"
]

COOKIES_PATH = "fb_cookies.json"
MIN_RESULTADOS = 20
MAX_RESULTADOS = 30
MINIMO_NUEVOS = 10
MAX_INTENTOS = 6
MIN_PRECIO_VALIDO = 3000

# Inicializar tabla base datos
inicializar_tabla_anuncios()


def limpiar_url(link: Optional[str]) -> str:
    """Limpia y normaliza la URL de Facebook Marketplace"""
    if not link:
        return ""
    clean_link = link.strip().replace('\n', '').replace('\r', '').replace(' ', '')
    path = urlparse(clean_link).path.rstrip('/')
    return f"https://www.facebook.com{path}"


async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    """Carga contexto Playwright con cookies o en modo anónimo"""
    logger.info("🔐 Cargando cookies desde GitHub Secret…")
    cookies_json = os.environ.get("FB_COOKIES_JSON", "")
    if not cookies_json:
        logger.warning("⚠️ FB_COOKIES_JSON no encontrado. Sesión anónima.")
        return await browser.new_context(locale="es-ES")
    try:
        with open(COOKIES_PATH, "w", encoding="utf-8") as f:
            f.write(cookies_json)
        context = await browser.new_context(
            storage_state=COOKIES_PATH,
            locale="es-ES",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        )
        logger.info("✅ Cookies restauradas desde storage_state.")
        return context
    except Exception as e:
        logger.error(f"Error al cargar cookies: {e}")
        return await browser.new_context(locale="es-ES")


async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extrae elementos del DOM con anuncio y su href"""
    items_data = []
    try:
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        logger.debug(f"Se encontraron {len(items)} items en la página")
        for a in items:
            texto = (await a.inner_text()).strip()
            href = await a.get_attribute("href")
            url = limpiar_url(href)
            items_data.append({"texto": texto, "url": url})
    except Exception as e:
        logger.error(f"Error extrayendo items de la página: {e}")
    return items_data


def extraer_anio_y_titulo(texto: str, modelo: str) -> Tuple[Optional[int], str]:
    """Intenta extraer año y título limpio del texto del anuncio"""
    lines = [l.strip() for l in texto.splitlines() if l.strip()]
    anio = None
    titulo = modelo.title()

    # Intentar extraer año en segunda línea
    if len(lines) > 1:
        try:
            posible_anio = int(lines[1].split()[0])
            if 1990 <= posible_anio <= 2030:
                anio = posible_anio
                titulo = " ".join(lines[1].split()[1:]).title() or titulo
        except (ValueError, IndexError):
            pass

    # Si no, buscar año con regex en todo el texto
    if not anio:
        match = re.search(r"\b(19[9]\d|20[0-2]\d|2030)\b", texto)
        if match:
            anio = int(match.group())

    return anio, titulo


async def hacer_scroll_pagina(page: Page, veces: int = 5, min_delay=0.8, max_delay=1.5):
    """Simula scroll para cargar más anuncios"""
    for _ in range(veces):
        await page.mouse.wheel(0, 400)
        await asyncio.sleep(random.uniform(min_delay, max_delay))


async def procesar_modelo(page: Page, modelo: str, resultados: List[str], pendientes_manual: List[str]) -> int:
    """
    Busca anuncios de un modelo, procesa y guarda resultados.
    Devuelve cantidad nuevos anuncios relevantes encontrados.
    """
    nuevos_urls = set()
    vistos = set()
    contador = {
        "total": 0,
        "duplicado": 0,
        "sin_precio": 0,
        "negativo": 0,
        "sin_anio": 0,
        "guardado": 0
    }

    url_busqueda = (
        f"https://www.facebook.com/marketplace/guatemala/search/"
        f"?query={modelo.replace(' ', '%20')}"
        f"&minPrice=1000&maxPrice=60000"
        f"&sortBy=best_match&conditions=used_good_condition"
    )
    await page.goto(url_busqueda)
    await asyncio.sleep(random.uniform(4, 7))

    nombre_usuario = "👤 Sesión anónima"
    if await page.query_selector("a[role='link'][href^='/me/']"):
        nombre_usuario = await page.inner_text("a[role='link'][href^='/me/']")
    logger.info(f"🙋 Usuario detectado: {nombre_usuario}")

    for intento in range(MAX_INTENTOS):
        logger.info(f"🔄 Intento {intento+1}/{MAX_INTENTOS}: buscando anuncios para {modelo.upper()}")
        items = await extraer_items_pagina(page)

        if not items:
            logger.warning(f"No se encontraron items en el intento {intento + 1} para {modelo}")
            await hacer_scroll_pagina(page)
            continue

        for item in items:
            texto = item["texto"]
            full_url = limpiar_link(item["url"])
            contador["total"] += 1

            if not full_url.startswith("https://www.facebook.com/marketplace/item/"):
                logger.warning(f"🚨 Link inválido detectado → {repr(full_url)}")
                continue
                
           if not texto or full_url in vistos or existe_en_db(full_url):
                contador["duplicado"] += 1
                continue
            vistos.add(full_url)

            if contiene_negativos(texto):
                contador["negativo"] += 1
                continue

            vistos.add(full_url)

            match_precio = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
            if not match_precio:
                contador["sin_precio"] += 1
                pendientes_manual.append(f"🔍 {modelo.title()}\n📝 {texto}\n📎 {full_url}")
                continue

            precio = limpiar_precio(match_precio.group())
            if precio < MIN_PRECIO_VALIDO:
                continue

            if not coincide_modelo(texto, modelo):
                continue

            anio, titulo = extraer_anio_y_titulo(texto, modelo)

            if not anio or precio == 0:
                contador["sin_anio"] += 1
                continue

            km = ""
            lines = [l.strip() for l in texto.splitlines() if l.strip()]
            if len(lines) > 3:
                km = lines[3]

            roi = calcular_roi_real(modelo, precio, anio)
            score = puntuar_anuncio(titulo, precio, texto)
            relevante = score >= 6 and roi >= -10

            logger.info(f"📝 Evaluando → Precio: Q{precio:,} | Año: {anio} | ROI: {roi:.1f}% | Score: {score}/10")

            if existe_en_db(full_url):
                logger.info(f"⚠️ Ya existe en base. Saltando → {full_url}")
                contador["duplicado"] += 1
                continue

            insertar_anuncio_db(
                url=full_url,
                modelo=modelo,
                año=anio,
                precio=precio,
                kilometraje=km,
                roi=roi,
                score=score,
                relevante=relevante
            )
            contador["guardado"] += 1

            if relevante:
                mensaje = (
                    f"🚘 *{titulo}*\n"
                    f"• Año: {anio}\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• Kilometraje: {km}\n"
                    f"• ROI: {roi:.1f}%\n"
                    f"• Score: {score}/10\n"
                    f"🔗 {full_url}"
                ).strip()
                nuevos_urls.add(full_url)
                resultados.append(mensaje)
            elif score >= 8:
                logger.info(f"🟡 Relevante pero ROI bajo. Score alto ({score}) pero ROI: {roi:.1f}% → {full_url}")
            else:
                logger.info(f"⛔ Descartado. Score: {score} | ROI: {roi:.1f}% → {full_url}")

        logger.info(f"📊 Diagnóstico para {modelo.upper()}: {contador}")

        if len(nuevos_urls) >= MINIMO_NUEVOS:
            logger.info(f"✅ Se encontraron {len(nuevos_urls)} nuevos para {modelo}")
            break

        # Scroll para cargar más anuncios
        await hacer_scroll_pagina(page)

    return len(nuevos_urls)


async def buscar_autos_marketplace() -> Tuple[List[str], List[str]]:
    """Función principal que coordina la búsqueda y retorna resultados y pendientes"""
    logger.info("\n🔎 Iniciando búsqueda en Marketplace…")
    resultados = []
    pendientes_manual = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        modelos = MODELOS_INTERES.copy()
        random.shuffle(modelos)

        for modelo in modelos:
            if len(resultados) >= MIN_RESULTADOS:
                break

            logger.info(f"\n🔍 Buscando modelo: {modelo.upper()}")
            nuevos = await procesar_modelo(page, modelo, resultados, pendientes_manual)

            if nuevos >= MINIMO_NUEVOS and len(resultados) >= MIN_RESULTADOS:
                logger.info("Alcanzado mínimo de resultados. Terminando búsqueda.")
                break

        await browser.close()

    return resultados, pendientes_manual


if __name__ == "__main__":
    async def main():
        brutos, pendientes = await buscar_autos_marketplace()
        for msg in brutos:
            print(msg + "\n")
        if pendientes:
            print("📌 Pendientes de revisión manual:")
            for p in pendientes:
                print(p + "\n")

    asyncio.run(main())
