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

# 🔧 Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 📌 Parámetros principales
MODELOS_INTERES = [
    "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
    "kia picanto", "chevrolet spark", "honda", "nissan march",
    "suzuki alto", "suzuki swift", "suzuki grand vitara",
    "hyundai accent", "hyundai i10", "kia rio"
]
COOKIES_PATH = "fb_cookies.json"
MIN_PRECIO_VALIDO = 3000
MAX_INTENTOS = 12

# 🏁 Inicializa la base de datos
inicializar_tabla_anuncios()

def limpiar_url(link: Optional[str]) -> str:
    if not link:
        return ""
    path = urlparse(link.strip().replace('\n', '').replace('\r', '').replace(' ', '')).path.rstrip('/')
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    logger.info("🔐 Cargando cookies desde entorno…")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
        return await browser.new_context(locale="es-ES")
    with open(COOKIES_PATH, "w", encoding="utf-8") as f:
        f.write(cj)
    return await browser.new_context(
        storage_state=COOKIES_PATH,
        locale="es-ES",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    try:
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        return [{
            "texto": (await a.inner_text()).strip(),
            "url": limpiar_url(await a.get_attribute("href"))
        } for a in items]
    except Exception as e:
        logger.error(f"❌ Error al extraer items: {e}")
        return []

def extraer_anio_y_titulo(texto: str, modelo: str) -> Tuple[Optional[int], str]:
    lines = [l.strip() for l in texto.splitlines() if l.strip()]
    anio = None
    titulo = modelo.title()
    if len(lines) > 1:
        try:
            posible = int(lines[1].split()[0])
            if 1990 <= posible <= 2030:
                anio = posible
                titulo = " ".join(lines[1].split()[1:]).title() or titulo
        except (ValueError, IndexError):
            pass
    if not anio:
        match = re.search(r"\b(19[9]\d|20[0-2]\d|2030)\b", texto)
        if match:
            anio = int(match.group())
    return anio, titulo

async def hacer_scroll_pagina(page: Page, veces=5, min_delay=0.8, max_delay=1.5):
    for _ in range(veces):
        await page.mouse.wheel(0, 400)
        await asyncio.sleep(random.uniform(min_delay, max_delay))

async def procesar_modelo(page: Page, modelo: str, resultados: List[str], pendientes: List[str]) -> int:
    vistos, nuevos = set(), set()
    contador = {k: 0 for k in ["total", "duplicado", "negativo", "sin_precio", "sin_anio", "filtro_modelo", "guardado"]}
    url_busqueda = (
        f"https://www.facebook.com/marketplace/guatemala/search/"
        f"?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000"
        f"&sortBy=best_match&conditions=used_good_condition"
    )
    await page.goto(url_busqueda)
    await asyncio.sleep(random.uniform(4, 7))
    for _ in range(MAX_INTENTOS):
        items = await extraer_items_pagina(page)
        if not items:
            await hacer_scroll_pagina(page)
            continue
        for item in items:
            texto, full_url = item["texto"], limpiar_link(item["url"])
            contador["total"] += 1
            if not full_url.startswith("https://www.facebook.com/marketplace/item/"):
                continue
            if not texto or full_url in vistos or existe_en_db(full_url):
                contador["duplicado"] += 1
                continue
            vistos.add(full_url)
            if contiene_negativos(texto):
                contador["negativo"] += 1
                continue
            m = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
            if not m:
                contador["sin_precio"] += 1
                pendientes.append(f"🔍 {modelo.title()}\n📝 {texto}\n📎 {full_url}")
                continue
            precio = limpiar_precio(m.group())
            if precio < MIN_PRECIO_VALIDO:
                continue
            anio, titulo = extraer_anio_y_titulo(texto, modelo)
            if not anio:
                contador["sin_anio"] += 1
                continue
            if not coincide_modelo(texto, modelo):
                score_test = puntuar_anuncio(titulo, precio, texto)
                if score_test < 7:
                    contador["filtro_modelo"] += 1
                    continue
            km = texto.splitlines()[3].strip() if len(texto.splitlines()) > 3 else ""
            roi = calcular_roi_real(modelo, precio, anio)
            score = puntuar_anuncio(titulo, precio, texto)
            insertar_anuncio_db(
                url=full_url,
                modelo=modelo,
                año=anio,
                precio=precio,
                kilometraje=km,
                roi=roi,
                score=score,
                relevante=(score >= 6 and roi >= -10),
                completo=1
            )
            contador["guardado"] += 1
            if score >= 6 and roi >= -10:
                mensaje = (
                    f"🚘 *{titulo}*\n"
                    f"• Año: {anio}\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• Kilometraje: {km}\n"
                    f"• ROI: {roi:.1f}%\n"
                    f"• Score: {score}/10\n"
                    f"🔗 {full_url}"
                )
                resultados.append(mensaje)
                nuevos.add(full_url)
        await hacer_scroll_pagina(page)
    logger.info(f"📊 {modelo.upper()} → {contador}")
    return len(nuevos)

async def buscar_autos_marketplace() -> Tuple[List[str], List[str]]:
    logger.info("\n🔎 Iniciando búsqueda en Marketplace…")
    resultados, pendientes = [], []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()
        for modelo in random.sample(MODELOS_INTERES, len(MODELOS_INTERES)):
            logger.info(f"\n🔍 Procesando modelo: {modelo.upper()}")
            await procesar_modelo(page, modelo, resultados, pendientes)
        await browser.close()
    return resultados, pendientes

if __name__ == "__main__":
    async def main():
        brutos, pendientes = await buscar_autos_marketplace()
        for r in brutos:
            print(r + "\n")
        if pendientes:
            print("📌 Pendientes para revisión manual:\n")
            for p in pendientes:
                print(p + "\n")
    asyncio.run(main())
