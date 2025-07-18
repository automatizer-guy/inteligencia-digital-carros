# scraper_marketplace.py

import os
import re
import json
import random
import asyncio
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo, extraer_anio,
    insertar_o_actualizar_anuncio_db, inicializar_tabla_anuncios,
    limpiar_link, MODELOS_INTERES,
    SCORE_MIN_TELEGRAM, ROI_MINIMO
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5
ROI_POTENCIAL_MIN = ROI_MINIMO - 10
DB_PATH = os.environ.get("DB_PATH", "upload-artifact/anuncios.db")


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


async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    try:
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        for a in items:
            titulo = (await a.inner_text()).strip()
            aria_label = await a.get_attribute("aria-label") or ""
            texto_completo = f"{titulo} {aria_label}".strip()
            href = await a.get_attribute("href") or ""
            resultados.append({"texto": texto_completo, "url": limpiar_link(href)})
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
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str],
    conn: sqlite3.Connection
) -> int:
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "nuevos_ins", "actualizados", "precio_bajo", "extranjero"
    ]}
    SORT_OPTS = ["best_match", "newest", "price_asc"]
    inicio = datetime.now()

    for sort in SORT_OPTS:
        url_busq = (
            f"https://www.facebook.com/marketplace/guatemala/search/"
            f"?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
        )
        await page.goto(url_busq)
        await asyncio.sleep(random.uniform(2, 4))

        scrolls = 0
        consec_repetidos = 0
        nuevos_set = set()

        while scrolls < 25:
            nuevos_scroll = 0
            items = await extraer_items_pagina(page)
            for itm in items:
                url = limpiar_link(itm["url"])
                texto = itm["texto"].strip()
                contador["total"] += 1

                if not url.startswith("https://www.facebook.com/marketplace/item/"):
                    continue
                if url in vistos_globales:
                    contador["duplicado"] += 1
                    continue
                vistos_globales.add(url)

                try:
                    await page.goto(url)
                    await asyncio.sleep(2)
                    texto = await page.inner_text("div[role='main']")
                except Exception as e:
                    logger.warning(f"Error accediendo a {url}: {e}")
                    texto = itm["texto"]

                texto = texto.strip()
                if not coincide_modelo(texto, modelo):
                    contador["filtro_modelo"] += 1
                    continue
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

                roi_data = calcular_roi_real(modelo, precio, anio)
                score = puntuar_anuncio(texto, roi_data)
                relevante = score >= SCORE_MIN_TELEGRAM and roi_data["roi"] >= ROI_MINIMO

                mensaje_base = (
                    f"🚘 *{modelo.title()}*\n"
                    f"• Año: {anio}\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• ROI: {roi_data['roi']:.2f}%\n"
                    f"• Score: {score}/10\n"
                    f"🔗 {url}"
                )

                resultado = insertar_o_actualizar_anuncio_db(
                    conn,
                    link=url,
                    modelo=modelo,
                    anio=anio,
                    precio=precio,
                    km="",
                    roi=roi_data["roi"],
                    score=score,
                    relevante=relevante,
                    confianza_precio=roi_data["confianza"],
                    muestra_precio=roi_data["muestra"]
                )
                if resultado == "nuevo":
                    contador["nuevos_ins"] += 1
                else:
                    contador["actualizados"] += 1

                logger.info(
                    f"💾 {resultado.title()}: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score} | Relevante={relevante}"
                )
                nuevos_set.add(url)
                nuevos_scroll += 1
                procesados.append(mensaje_base)

                if relevante:
                    relevantes.append(mensaje_base)
                elif ROI_POTENCIAL_MIN <= roi_data["roi"] < ROI_MINIMO:
                    potenciales.append(mensaje_base)

            scrolls += 1
            if nuevos_scroll == 0:
                consec_repetidos += 1
            else:
                consec_repetidos = 0
            if consec_repetidos >= 5 and len(nuevos_set) < 5:
                break
            if not await scroll_hasta(page):
                break

    duracion = (datetime.now() - inicio).seconds
    logger.info(f"✨ MODELO: {modelo.upper()} "
                f"Duración: {duracion}s | Total: {contador['total']} | Guardados: {contador['nuevos_ins'] + contador['actualizados']} "
                f"| Nuevos: {contador['nuevos_ins']} | Actualizados: {contador['actualizados']} | Duplicados: {contador['duplicado']}")

    return len(nuevos_set)


async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    inicializar_tabla_anuncios()
    modelos = modelos_override or MODELOS_INTERES
    flops = modelos_bajo_rendimiento()
    activos = [m for m in modelos if m not in flops]

    procesados, potenciales, relevantes = [], [], []

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        await page.goto("https://www.facebook.com/marketplace")
        await asyncio.sleep(3)

        if "login" in page.url or "recover" in page.url:
            alerta = (
                "🚨 Sesión inválida: redirigido a la página de inicio. "
                "Verifica las cookies (FB_COOKIES_JSON)."
            )
            logger.warning(alerta)
            conn.close()
            return [], [], [alerta]

        logger.info("✅ Sesión activa detectada correctamente en Marketplace.")

        for m in random.sample(activos, len(activos)):
            try:
                await asyncio.wait_for(
                    procesar_modelo(page, m, procesados, potenciales, relevantes, conn),
                    timeout=420
                )
            except asyncio.TimeoutError:
                logger.warning(f"⏳ {m} → Excedió tiempo máximo. Se aborta.")

        await browser.close()

    conn.close()

    return procesados, potenciales, relevantes


if __name__ == "__main__":
    async def main():
        procesados, potenciales, relevantes = await buscar_autos_marketplace()

        logger.info("📦 Resumen final del scraping")
        logger.info(f"Guardados totales: {len(procesados)}")
        logger.info(f"Relevantes: {len(relevantes)}")
        logger.info(f"Potenciales: {len(potenciales)}")

    asyncio.run(main())
