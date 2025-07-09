import re
import sqlite3
import os
import asyncio
import logging
import time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo,
    existe_en_db, inicializar_tabla_anuncios, limpiar_link,
    MODELOS_INTERES
)

# —— Configuración de logging ——
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# —— Parámetros de búsqueda ——
COOKIES_PATH       = "fb_cookies.json"
MIN_PRECIO_VALIDO  = 3000
MAX_INTENTOS       = 8
SORT_OPTS          = ["best_match", "newest", "price_asc"]
MAX_TIEMPO_MODELO  = 120  # segundos de timeout por modelo
BATCH_INSERT_SIZE  = 50   # cuantos registros acumular antes de flush

# —— Inicialización DB & esquema incremental ——
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
inicializar_tabla_anuncios()
# Tabla auxiliar para tracking incremental
with sqlite3.connect(DB_PATH) as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS progreso (
            modelo TEXT PRIMARY KEY,
            ultima_url TEXT,
            timestamp_scrape TEXT
        )
    """)

# —— Helpers de retry/backoff ——
async def retry_async(fn, *args, retries=3, base_delay=1, **kwargs):
    delay = base_delay
    for attempt in range(1, retries+1):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            if attempt == retries:
                raise
            logger.warning(f"⚠️ Retry {attempt}/{retries} tras error: {e}")
            await asyncio.sleep(delay)
            delay *= 2

# —— Extracción de items con retry ——
async def extraer_items_pagina(page: Page) -> List[Dict[str,str]]:
    async def _extract():
        els = await page.query_selector_all("a[href*='/marketplace/item']")
        data = []
        for a in els:
            txt = (await a.inner_text()).strip()
            url = limpiar_url(await a.get_attribute("href"))
            data.append({"texto": txt, "url": url})
        return data
    return await retry_async(_extract, retries=2, base_delay=0.5)

# —— Scroll seguro —— 
async def hacer_scroll_pagina(page: Page):
    await page.mouse.wheel(0, 400)
    await asyncio.sleep(0.5 + random.random())

# —— Procesado de un modelo con timeout, batch insert e incremental ——
async def procesar_modelo(page: Page, modelo: str,
                          resultados: List[str], pendientes: List[str]) -> int:
    logger.info(f"🔍 Inicio modelo {modelo.upper()}")
    inicio_t = time.time()

    # extraer última URL procesada
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT ultima_url FROM progreso WHERE modelo = ?", (modelo,)
        ).fetchone()
    ultima_url = row[0] if row else None

    nuevos    = set()
    batch_buf = []
    contador  = {k: 0 for k in ["total","guardado","relevantes"]}

    async def flush_batch():
        nonlocal batch_buf
        if not batch_buf: return
        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO anuncios "
                "(link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch_buf
            )
            conn.commit()
        batch_buf.clear()

    for sort in SORT_OPTS:
        url_base = (
          "https://www.facebook.com/marketplace/guatemala/search/"
          f"?query={modelo.replace(' ','%20')}&minPrice=1000&maxPrice=60000"
          f"&sortBy={sort}"
        )
        await retry_async(page.goto, url_base, retries=2, base_delay=1)
        await asyncio.sleep(1 + random.random())

        intento = 0
        while intento < MAX_INTENTOS:
            # Timeout chequeo
            if time.time() - inicio_t > MAX_TIEMPO_MODELO:
                logger.warning(f"⏱ Timeout para {modelo}; abortando.")
                await flush_batch(); return len(nuevos)

            items = await extraer_items_pagina(page)
            if not items:
                await hacer_scroll_pagina(page)
                intento += 1
                continue

            for item in items:
                txt, url = item["texto"], limpiar_link(item["url"])
                contador["total"] += 1

                if url == ultima_url:
                    logger.info(f"⏭ Llegamos a último scrapeado ({url}), stop.")
                    await flush_batch(); 
                    # guardamos progreso final y sale
                    with sqlite3.connect(DB_PATH) as c:
                        c.execute(
                            "REPLACE INTO progreso(modelo,ultima_url,timestamp_scrape) VALUES(?,?,?)",
                            (modelo, next(iter(nuevos), ultima_url), datetime.now().isoformat())
                        )
                    return len(nuevos)

                if not url.startswith("https://www.facebook.com/marketplace/item/"):
                    continue
                if existe_en_db(url) or contiene_negativos(txt):
                    continue
                m = re.search(r"[Qq\$]\s?[\d\.,]+", txt)
                if not m:
                    pendientes.append(f"🔍 {modelo}\n📝 {txt}\n📎 {url}")
                    continue

                precio = limpiar_precio(m.group())
                if precio < MIN_PRECIO_VALIDO:
                    continue

                anio, titulo = extraer_anio_y_titulo(txt, modelo)
                if not anio: continue

                roi   = calcular_roi_real(modelo, precio, anio)
                score = puntuar_anuncio(titulo, precio, txt)
                relevante = (score>=6 and roi>=10)

                batch_buf.append((
                    url, modelo, anio, precio, "", datetime.now().isoformat(),
                    roi, score, int(relevante)
                ))
                contador["guardado"] += 1
                if relevante:
                    contador["relevantes"] += 1
                    nuevos.add(url)
                    resultados.append((
                        f"🚘 *{titulo}*\n"
                        f"• Año: {anio}\n• Precio: Q{precio:,}\n"
                        f"• ROI: {roi:.1f}% | Score: {score}/10\n"
                        f"🔗 {url}"
                    ))

                # flush a intervalos
                if len(batch_buf) >= BATCH_INSERT_SIZE:
                    await flush_batch()

            intento += 1
            await hacer_scroll_pagina(page)

    # flush final
    await flush_batch()
    # guardar progreso
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "REPLACE INTO progreso(modelo,ultima_url,timestamp_scrape) VALUES(?,?,?)",
            (modelo, next(iter(nuevos), ultima_url), datetime.now().isoformat())
        )

    t_total = time.time() - inicio_t
    logger.info(f"📊 {modelo}: total={contador['total']} guardados={contador['guardado']} "
                f"relevantes={contador['relevantes']} en {t_total:.1f}s")
    return len(nuevos)

# —— Función principal de scraping —— 
async def buscar_autos_marketplace(
    modelos_override: Optional[List[str]] = None
) -> Tuple[List[str], List[str]]:
    logger.info("🔎 Iniciando búsqueda en Marketplace…")
    resultados, pendientes = [], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx     = await cargar_contexto_con_cookies(browser)
        page    = await ctx.new_page()

        # omitimos flops
        activos = modelos_override or MODELOS_INTERES
        # modelo_bajo_rendimiento import si lo tienes
        # activos = [m for m in activos if m not in modelos_bajo_rendimiento()]

        for modelo in random.sample(activos, len(activos)):
            await procesar_modelo(page, modelo, resultados, pendientes)

        await browser.close()

    return resultados, pendientes

# —— Para pruebas standalone —— 
if __name__ == "__main__":
    import asyncio
    br, pe = asyncio.run(buscar_autos_marketplace())
    for r in br: print(r+"\n")
    if pe:
        print("📌 Pendientes:\n", "\n".join(pe))
