import re
import os
import random
import asyncio
import logging
from urllib.parse import urlparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo, extraer_anio,
    existe_en_db, insertar_anuncio_db, inicializar_tabla_anuncios,
    limpiar_link, modelos_bajo_rendimiento, MODELOS_INTERES,
    SCORE_MIN_TELEGRAM, ROI_MINIMO
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

COOKIES_PATH = "fb_cookies.json"
MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    logger.info("🔐 Cargando cookies desde entorno…")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
        return await browser.new_context(locale="es-ES")
    # Guardamos en disco la variable de entorno
    with open(COOKIES_PATH, "w", encoding="utf-8") as f:
        f.write(cj)
    # Creamos contexto con esas cookies
    return await browser.new_context(
        storage_state=COOKIES_PATH,
        locale="es-ES",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )

def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    try:
        anchors = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        for a in anchors:
            titulo = (await a.inner_text()).strip()
            aria = await a.get_attribute("aria-label") or ""
            href = await a.get_attribute("href") or ""
            resultados.append({
                "texto": f"{titulo} {aria}".strip(),
                "url": limpiar_url(href)
            })
        return resultados
    except Exception as e:
        logger.error(f"❌ Error al extraer items: {e}")
        return []

async def scroll_hasta(page: Page) -> bool:
    prev = await page.evaluate("document.body.scrollHeight")
    await page.mouse.wheel(0, 400)
    await asyncio.sleep(random.uniform(0.8, 1.2))
    now = await page.evaluate("document.body.scrollHeight")
    return now > prev

async def procesar_modelo(
    page: Page,
    modelo: str,
    resultados: List[str],
    pendientes: List[str],
    destacados: List[str]
) -> int:
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero"
    ]}
    SORT_OPTS = ["best_match", "newest", "price_asc"]

    for sort in SORT_OPTS:
        url_busq = (
            "https://www.facebook.com/marketplace/guatemala/search/"
            f"?query={modelo.replace(' ', '%20')}"
            f"&minPrice=1000&maxPrice=60000&sortBy={sort}"
        )
        await page.goto(url_busq)
        await asyncio.sleep(random.uniform(2, 4))

        scrolls = 0
        consec_dup = 0
        nuevos = set()

        while scrolls < 25:
            items = await extraer_items_pagina(page)
            logger.info(f"🧩 {modelo} ({sort}) — Scroll #{scrolls+1}: {len(items)} ítems encontrados")

            nuevos_en_scroll = 0
            for itm in items:
                url = itm["url"]
                contador["total"] += 1

                if not url.startswith("https://www.facebook.com/marketplace/item/"):
                    continue
                if url in vistos_globales or existe_en_db(url):
                    contador["duplicado"] += 1
                    consec_dup += 1
                    vistos_globales.add(url)
                    continue

                # volcamos la página individual
                try:
                    await page.goto(url)
                    await asyncio.sleep(2)
                    texto = await page.inner_text("div[role='main']")
                except:
                    texto = itm["texto"]

                texto = texto.strip()
                if contiene_negativos(texto):
                    contador["negativo"] += 1
                    continue
                if "mexico" in texto.lower():
                    contador["extranjero"] += 1
                    continue

                m_pr = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
                if not m_pr:
                    contador["sin_precio"] += 1
                    pendientes.append(f"🔍 {modelo.title()}\n📝 {texto}\n🔗 {url}")
                    continue

                precio = limpiar_precio(m_pr.group())
                if precio < MIN_PRECIO_VALIDO:
                    contador["precio_bajo"] += 1
                    continue

                anio = extraer_anio(texto)
                if not anio or not (1990 <= anio <= datetime.now().year):
                    contador["sin_anio"] += 1
                    if len(sin_anio_ejemplos) < MAX_EJEMPLOS_SIN_ANIO:
                        sin_anio_ejemplos.append((texto, url))
                    continue

                if not coincide_modelo(texto, modelo):
                    score_t = puntuar_anuncio(texto)
                    if score_t < SCORE_MIN_TELEGRAM:
                        contador["filtro_modelo"] += 1
                        continue

                roi   = calcular_roi_real(modelo, precio, anio)
                score = puntuar_anuncio(texto)
                insertar_anuncio_db(url, modelo, anio, precio, "", roi, score, relevante=False)
                contador["guardado"] += 1
                nuevos.add(url)
                nuevos_en_scroll += 1

                # solo destacamos si cumple ambos
                if score >= SCORE_MIN_TELEGRAM and roi >= ROI_MINIMO:
                    msg = f"🚘 *{modelo.title()}* | Año: {anio} | Precio: Q{precio:,} | ROI: {roi:.1f}% | Score: {score}/10\n🔗 {url}"
                    resultados.append(msg)
                    destacados.append(msg)

            scrolls += 1
            if nuevos_en_scroll == 0:
                consec_dup += 1
            else:
                consec_dup = 0

            if consec_dup >= 5 and len(nuevos) < 5:
                break
            if not await scroll_hasta(page):
                break

    logger.info(f"📊 {modelo.upper()} → {contador}")

    if sin_anio_ejemplos:
        logger.info(f"📌 Ejemplos sin año para {modelo}:")
        for i, (t, u) in enumerate(sin_anio_ejemplos, 1):
            logger.info(f"   {i}. {t[:80]}… | {u}")

    return len(nuevos)

async def buscar_autos_marketplace(
    modelos_override: Optional[List[str]] = None
) -> Tuple[List[str], List[str], List[str]]:
    inicializar_tabla_anuncios()
    modelos = modelos_override or MODELOS_INTERES
    flops   = modelos_bajo_rendimiento()
    activos = [m for m in modelos if m not in flops]

    results, pend, destacados = [], [], []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx     = await cargar_contexto_con_cookies(browser)
        page    = await ctx.new_page()

        # 1) diagnóstico de sesión
        await page.goto("https://www.facebook.com/marketplace")
        await asyncio.sleep(3)
        try:
            # si aparece esta cadena, es login
            await page.wait_for_selector("text=Crear cuenta nueva", timeout=5000)
            alerta = "🚨 SESIÓN NO VÁLIDA EN MARKETPLACE. Revisa tus cookies."
            logger.warning(alerta)
            return [alerta], [alerta], [alerta]
        except:
            logger.info("✅ Sesión activa detectada en Marketplace.")

        # 2) procesamos cada modelo
        for m in random.sample(activos, len(activos)):
            try:
                await asyncio.wait_for(
                    procesar_modelo(page, m, results, pend, destacados),
                    timeout=420
                )
            except asyncio.TimeoutError:
                logger.warning(f"⏳ {m} → timeout. Se salta.")

        await browser.close()
    return results, pend, destacados

if __name__ == "__main__":
    async def main():
        brutos, pendientes, relevantes = await buscar_autos_marketplace()
        # salida por consola
        for r in brutos:
            print(r, "\n")
        if pendientes:
            print("📌 Pendientes:\n")
            for p in pendientes:
                print(p, "\n")
        if relevantes:
            print("📦 Destacados:\n")
            for d in relevantes:
                print(d, "\n")
        else:
            print("😕 No se encontró ningún anuncio destacado.\n")

    asyncio.run(main())
