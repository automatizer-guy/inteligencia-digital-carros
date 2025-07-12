import os
import re
import json
import random
import asyncio
import logging
from urllib.parse import urlparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo, extraer_anio,
    existe_en_db, insertar_anuncio_db, inicializar_tabla_anuncios,
    limpiar_link, modelos_bajo_rendimiento, MODELOS_INTERES,
    SCORE_MIN_TELEGRAM, ROI_MINIMO
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5
ROI_POTENCIAL_MIN = ROI_MINIMO - 10

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    logger.info("🔐 Cargando cookies desde entorno…")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
        return await browser.new_context(locale="es-ES")

    try:
        cookies = json.loads(cj)
    except Exception as e:
        logger.error(f"❌ Error al parsear FB_COOKIES_JSON: {e}")
        return await browser.new_context(locale="es-ES")

    context = await browser.new_context(
        locale="es-ES",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    )
    await context.add_cookies(cookies)
    return context

def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    try:
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        for a in items:
            titulo = (await a.inner_text()).strip()
            aria_label = await a.get_attribute("aria-label") or ""
            texto_completo = f"{titulo} {aria_label}".strip()
            href = await a.get_attribute("href") or ""
            resultados.append({"texto": texto_completo, "url": limpiar_url(href)})
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

async def procesar_modelo(page: Page, modelo: str,
                          procesados: List[str],
                          potenciales: List[str],
                          relevantes: List[str]) -> int:
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero"
    ]}
    SORT_OPTS = ["best_match", "newest", "price_asc"]
    inicio = datetime.now()

    for sort in SORT_OPTS:
        url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
        await page.goto(url_busq)
        await asyncio.sleep(random.uniform(2, 4))

        scrolls_realizados = 0
        consec_repetidos = 0
        nuevos = set()

        while scrolls_realizados < 25:
            items = await extraer_items_pagina(page)
            logger.info(f"🧹 {modelo} ({sort}) — Scroll #{scrolls_realizados+1}: {len(items)} ítems encontrados")

            nuevos_en_scroll = 0

            for itm in items:
                url = limpiar_link(itm["url"])
                contador["total"] += 1

                if not url.startswith("https://www.facebook.com/marketplace/item/"):
                    continue
                if url in vistos_globales or existe_en_db(url):
                    contador["duplicado"] += 1
                    consec_repetidos += 1
                    vistos_globales.add(url)
                    continue

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

                m = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
                if not m:
                    contador["sin_precio"] += 1
                    continue

                precio = limpiar_precio(m.group())
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

                roi = calcular_roi_real(modelo, precio, anio)
                score = puntuar_anuncio(texto)
                insertar_anuncio_db(url, modelo, anio, precio, "", roi, score, relevante=False)
                contador["guardado"] += 1
                nuevos.add(url)
                nuevos_en_scroll += 1

                mensaje_base = (
                    f"🚘 *{modelo.title()}*\n"
                    f"• Año: {anio}\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• ROI: {roi:.1f}%\n"
                    f"• Score: {score}/10\n"
                    f"🔗 {url}"
                )

                procesados.append(mensaje_base)

                if score >= SCORE_MIN_TELEGRAM and roi >= ROI_MINIMO:
                    relevantes.append(mensaje_base)
                elif ROI_POTENCIAL_MIN <= roi < ROI_MINIMO:
                    potenciales.append(mensaje_base)

            scrolls_realizados += 1
            if nuevos_en_scroll == 0:
                consec_repetidos += 1
            else:
                consec_repetidos = 0
            if consec_repetidos >= 5 and len(nuevos) < 5:
                break
            if not await scroll_hasta(page):
                break

    duracion = (datetime.now() - inicio).seconds
    resumen_vertical = f"""
✨ MODELO: {modelo.upper()}
   Duración: {duracion} s
   Total encontrados: {contador['total']}
   Guardados: {contador['guardado']}
   Relevantes: {len(relevantes)}
   Potenciales: {len(potenciales)}
   Duplicados: {contador['duplicado']}
   Desc. por score/modelo: {contador['filtro_modelo']}
   Precio bajo: {contador['precio_bajo']}
   Sin año: {contador['sin_anio']}
   Negativos: {contador['negativo']}
   Extranjero: {contador['extranjero']}
✨"""
    logger.info(resumen_vertical)
    return len(nuevos)

async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    inicializar_tabla_anuncios()
    modelos = modelos_override or MODELOS_INTERES
    flops = modelos_bajo_rendimiento()
    activos = [m for m in modelos if m not in flops]

    procesados, potenciales, relevantes = [], [], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        await page.goto("https://www.facebook.com/marketplace")
        await asyncio.sleep(3)

        if "login" in page.url or "recover" in page.url:
            alerta = "🚨 Sesión inválida: redirigido a la página de inicio de sesión. Verifica las cookies (FB_COOKIES_JSON)."
            logger.warning(alerta)
            return [], [], [alerta]

        logger.info("✅ Sesión activa detectada correctamente en Marketplace.")

        for m in random.sample(activos, len(activos)):
            try:
                await asyncio.wait_for(procesar_modelo(page, m, procesados, potenciales, relevantes), timeout=420)
            except asyncio.TimeoutError:
                logger.warning(f"⏳ {m} → Excedió tiempo máximo. Se aborta.")

        await browser.close()

    return procesados, potenciales, relevantes

if __name__ == "__main__":
    async def main():
        procesados, potenciales, relevantes = await buscar_autos_marketplace()

        if relevantes:
            print("🚀 Relevantes para Telegram:\n")
            for r in relevantes:
                print(r + "\n")
        else:
            mensaje_final = (
                f"📉 Hoy no se encontraron anuncios relevantes.\n"
                f"📦 Anuncios guardados: {len(procesados)} nuevos\n"
            )
            print(mensaje_final + "\n")

        if procesados:
            print("📂 Procesados:\n")
            for p in procesados:
                print(p + "\n")

        if potenciales:
            print("🎯 Potenciales cercanos a enviar:\n")
            for pot in potenciales:
                print(pot + "\n")

    asyncio.run(main())
