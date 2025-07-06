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

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constantes
MODELOS_INTERES = [
    "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
    "kia picanto", "chevrolet spark", "honda", "nissan march",
    "suzuki alto", "suzuki swift", "suzuki grand vitara",
    "hyundai accent", "hyundai i10", "kia rio"
]
COOKIES_PATH = "fb_cookies.json"
MIN_RESULTADOS = 20
MINIMO_NUEVOS = 10
MAX_INTENTOS = 6
MIN_PRECIO_VALIDO = 3000

# Iniciar DB
inicializar_tabla_anuncios()

def limpiar_url(link: Optional[str]) -> str:
    if not link:
        return ""
    clean = link.strip().replace('\n','').replace('\r','').replace(' ','')
    path = urlparse(clean).path.rstrip('/')
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    logger.info("🔐 Cargando cookies…")
    cj = os.environ.get("FB_COOKIES_JSON","")
    if not cj:
        logger.warning("Sesión anónima")
        return await browser.new_context(locale="es-ES")
    with open(COOKIES_PATH,"w") as f: f.write(cj)
    return await browser.new_context(storage_state=COOKIES_PATH, locale="es-ES")

async def extraer_items_pagina(page: Page) -> List[Dict[str,str]]:
    items = []
    try:
        els = await page.query_selector_all("a[href*='/marketplace/item']")
        for e in els:
            text = (await e.inner_text()).strip()
            href = await e.get_attribute("href")
            url = limpiar_url(href)
            items.append({"texto": text, "url": url})
    except Exception as e:
        logger.error(f"Error extraer_items: {e}")
    return items

async def hacer_scroll_pagina(page: Page):
    for _ in range(5):
        await page.mouse.wheel(0,400)
        await asyncio.sleep(random.uniform(0.8,1.5))

def extraer_anio(texto: str) -> Optional[int]:
    lines = [l for l in texto.splitlines() if l.strip()]
    # intento rápido
    if len(lines)>1 and lines[1].split()[0].isdigit():
        ano=int(lines[1].split()[0])
        if 1990<=ano<=2030:
            return ano
    m=re.search(r"\b(19[9]\d|20[0-2]\d|2030)\b", texto)
    return int(m.group()) if m else None

async def procesar_modelo(page: Page, modelo: str, resultados: List[str], pendientes: List[str]) -> int:
    nuevos=set(); vistos=set()
    url_search = (
        f"https://www.facebook.com/marketplace/guatemala/search/"
        f"?query={modelo.replace(' ','%20')}&minPrice=1000&maxPrice=60000"
    )
    await page.goto(url_search)
    await asyncio.sleep(random.uniform(4,7))

    for intento in range(MAX_INTENTOS):
        items = await extraer_items_pagina(page)
        if not items:
            await hacer_scroll_pagina(page)
            continue
        for itm in items:
            txt, url = itm["texto"], itm["url"]
            if not url.startswith("https://www.facebook.com/marketplace/item/"): continue
            if url in vistos or existe_en_db(url): continue
            vistos.add(url)
            # *** FILTROS MÍNIMOS PARA GUARDAR ***
            if contiene_negativos(txt): continue
            mprice = re.search(r"[Qq\$]\s?[\d\.,]+", txt)
            if not mprice: 
                pendientes.append(f"{modelo}: sin precio → {url}")
                continue
            precio = limpiar_precio(mprice.group())
            if precio < MIN_PRECIO_VALIDO: continue
            if not coincide_modelo(txt,modelo): continue
            ano = extraer_anio(txt)
            if not ano: continue
            km = txt.splitlines()[3] if len(txt.splitlines())>3 else ""
            # ✅ Guardar TODO anuncio válido
            roi = calcular_roi_real(modelo,precio,ano)
            score = puntuar_anuncio(modelo.title(), precio, txt)
            relevante = (score>=6 and roi>=10)
            insertar_anuncio_db(url, modelo, ano, precio, km, roi, score, relevante)
            # Sólo guardamos en resultados los RELEVANTES
            if relevante:
                msg = (
                    f"🚘 *{modelo.title()}* ({ano})\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• ROI: {roi:.1f}%  Score: {score}/10\n"
                    f"🔗 {url}"
                )
                nuevos.add(url)
                resultados.append(msg)
        if len(nuevos)>=MINIMO_NUEVOS:
            break
        await hacer_scroll_pagina(page)

    return len(nuevos)

async def buscar_autos_marketplace():
    resultados, pendientes = [], []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()
        modelos = MODELOS_INTERES.copy()
        random.shuffle(modelos)
        for mod in modelos:
            if len(resultados)>=MIN_RESULTADOS: break
            await procesar_modelo(page, mod, resultados, pendientes)
        await browser.close()
    return resultados, pendientes

# Para pruebas manuales
if __name__=="__main__":
    import asyncio
    res, pen = asyncio.run(buscar_autos_marketplace())
    print("\n-- RESULTADOS --")
    for r in res: print(r)
    if pen: print("\n-- PENDIENTES MANUALES --")
    for p in pen: print(p)
