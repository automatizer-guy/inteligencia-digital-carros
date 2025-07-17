import os
import re
import json
import random
import asyncio
import logging
import sqlite3
from urllib.parse import urlparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.async_api import async_playwright, Browser, Page, BrowserContext

from utils_analisis import (
    limpiar_precio,
    contiene_negativos,
    puntuar_anuncio,
    calcular_roi_real,
    coincide_modelo,
    extraer_anio,
    inicializar_tabla_anuncios,
    limpiar_link,
    modelos_bajo_rendimiento,
    MODELOS_INTERES,
    Config,
    validar_anuncio_completo,
    insertar_anuncio_en_db
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Sincronizar ruta de la base de datos
DB_PATH = os.path.abspath(os.environ.get("DB_PATH", "upload-artifact/anuncios.db"))
Config.DB_PATH = DB_PATH

MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5
ROI_POTENCIAL_MIN = Config.ROI_MINIMO - 10


def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"


async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    context = await browser.new_context()
    cookies_path = "cookies_marketplace.json"
    if os.path.exists(cookies_path):
        with open(cookies_path, "r") as f:
            cookies = json.load(f)
            await context.add_cookies(cookies)
            logger.info("Cookies cargadas correctamente.")
    else:
        logger.warning("No se encontraron cookies.")
    return context


async def extraer_enlaces(page: Page, modelo: str, max_scrolls: int = 10) -> List[str]:
    enlaces = set()
    contador_scroll = 0
    while contador_scroll < max_scrolls:
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(2, 4))
        nuevos = await page.query_selector_all("a[href*='/marketplace/item/']")
        for e in nuevos:
            href = await e.get_attribute("href")
            if href:
                enlaces.add(limpiar_url(href))
        contador_scroll += 1
        logger.info(f"Scroll {contador_scroll}/{max_scrolls} - Links acumulados: {len(enlaces)}")
    return list(enlaces)


async def extraer_datos_del_anuncio(page: Page) -> Dict[str, str]:
    await asyncio.sleep(2)
    elementos = await page.query_selector_all("div[aria-label='Contenido de la página']")
    texto_total = ""
    for el in elementos:
        txt = await el.inner_text()
        texto_total += txt + "\n"

    patrones = {
        "precio_texto": r"(Q[\d\.,]+|\$[\d\.,]+)",
        "titulo": r"^(.*?)\n",
        "descripcion": r"Descripción\n(.*?)\n",
        "ubicacion": r"Ubicación\n(.*?)\n"
    }

    datos = {}
    for campo, patron in patrones.items():
        match = re.search(patron, texto_total, re.MULTILINE | re.IGNORECASE | re.DOTALL)
        datos[campo] = match.group(1).strip() if match else ""

    datos["texto_crudo"] = texto_total
    return datos


async def analizar_enlace(context: BrowserContext, link: str, modelo: str) -> Optional[Dict[str, str]]:
    page = await context.new_page()
    try:
        await page.goto(link)
        await asyncio.sleep(2)
        datos = await extraer_datos_del_anuncio(page)
        await page.close()
    except Exception as e:
        logger.warning(f"Error al analizar {link}: {e}")
        await page.close()
        return None

    precio = limpiar_precio(datos.get("precio_texto", ""))
    texto = datos.get("titulo", "") + " " + datos.get("descripcion", "")
    anio = extraer_anio(texto)
    texto_lower = texto.lower()

    # Crear estructura básica del anuncio
    anuncio = {
        "link": link,
        "precio": precio,
        "modelo": modelo,
        "anio": anio,
        "descripcion": datos.get("descripcion", ""),
        "texto_crudo": datos.get("texto_crudo", ""),
        "score": 0,
        "roi": 0,
        "motivo": "",
        "km": "",
        "confianza_precio": "baja",
        "muestra_precio": 0,
        "relevante": 0
    }

    # Validación básica del anuncio
    valido, motivo = validar_anuncio_completo(
        texto=anuncio["texto_crudo"],
        precio=precio,
        anio=anio,
        modelo=modelo
    )
    if not valido:
        anuncio["motivo"] = motivo
        return anuncio

    # Filtros adicionales
    if contiene_negativos(texto_lower):
        anuncio["motivo"] = "contiene palabras negativas"
        return anuncio

    if not coincide_modelo(texto_lower, modelo):
        anuncio["motivo"] = "no coincide modelo"
        return anuncio

    # Calcular ROI y puntuación
    roi_data = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio(anuncio["texto_crudo"], roi_data)

    # Actualizar anuncio con los datos calculados
    anuncio.update({
        "score": score,
        "roi": roi_data["roi"],
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "relevante": 1 if score >= Config.SCORE_MIN_TELEGRAM and roi_data["roi"] >= Config.ROI_MINIMO else 0
    })

    # Determinar motivo final
    if score < Config.SCORE_MIN_TELEGRAM:
        anuncio["motivo"] = "score insuficiente"
    elif roi_data["roi"] < ROI_POTENCIAL_MIN:
        anuncio["motivo"] = "ROI bajo"
    else:
        anuncio["motivo"] = "candidato válido"

    return anuncio


async def main_scraper():
    modelo = os.environ.get("MODELO_OBJETIVO", "civic")
    max_scrolls = int(os.environ.get("MAX_SCROLLS", "12"))

    logger.info(f"Modelo objetivo: {modelo}")
    logger.info("Iniciando scraping Marketplace...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await cargar_contexto_con_cookies(browser)
        page = await context.new_page()

        await page.goto(f"https://www.facebook.com/marketplace/search/?query={modelo}")
        logger.info("Cargando página principal Marketplace...")
        enlaces = await extraer_enlaces(page, modelo, max_scrolls=max_scrolls)

        logger.info(f"Se encontraron {len(enlaces)} enlaces únicos.")
        await browser.close()

        browser = await p.chromium.launch(headless=False)
        context = await cargar_contexto_con_cookies(browser)

        # Inicializar la tabla de anuncios
        inicializar_tabla_anuncios()
        anuncios_procesados = []
        metricas = {
            "total": 0,
            "validos": 0,
            "descartados": 0
        }

        for link in enlaces:
            metricas["total"] += 1
            anuncio = await analizar_enlace(context, link, modelo)
            if anuncio:
                try:
                    # Insertar en la base de datos
                    insertar_anuncio_en_db(anuncio)
                    anuncios_procesados.append(anuncio)
                    
                    if anuncio["motivo"] == "candidato válido":
                        metricas["validos"] += 1
                    else:
                        metricas["descartados"] += 1
                        
                    logger.info(f"{link} → {anuncio['motivo']} | ROI={anuncio['roi']} | Score={anuncio['score']}")
                except Exception as e:
                    logger.error(f"Error insertando anuncio {link}: {e}")
            else:
                metricas["descartados"] += 1

        await browser.close()
        logger.info(f"Scraping finalizado. Total procesados: {len(anuncios_procesados)}")
        logger.info(f"Métricas de sesión: {metricas}")


if __name__ == "__main__":
    asyncio.run(main_scraper())
