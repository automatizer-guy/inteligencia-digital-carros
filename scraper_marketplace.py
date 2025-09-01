# scraper_marketplace.py

import os
import re
import json
import random
import asyncio
import logging
from urllib.parse import urlparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo, extraer_anio,
    existe_en_db, insertar_anuncio_db, inicializar_tabla_anuncios,
    limpiar_link, modelos_bajo_rendimiento, MODELOS_INTERES,
    SCORE_MIN_TELEGRAM, ROI_MINIMO, obtener_anuncio_db, anuncio_diferente
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5
ROI_POTENCIAL_MIN = ROI_MINIMO - 10

# Configuración con más variabilidad humana
BASE_SCROLLS = 15
SCROLL_VARIATION = 5  # ±5 scrolls aleatorios
MIN_DELAY = 2.5  # Ligeramente aumentado para ser más humano
MAX_DELAY = 5.5
DELAY_ENTRE_ANUNCIOS = (2.0, 4.0)  # Rango variable
MAX_CONSECUTIVOS_SIN_NUEVOS = 3
BATCH_SIZE_SCROLL = (6, 10)  # Rango variable

# Pool de User Agents realistas
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
]

def obtener_delay_humano(base_min: float, base_max: float, factor_fatiga: float = 0) -> float:
    """Genera delays más humanos con ligera variación por 'fatiga'"""
    base_delay = random.uniform(base_min, base_max)
    fatiga_extra = factor_fatiga * random.uniform(0, 0.5)  # Máximo 0.5s extra por fatiga
    return base_delay + fatiga_extra

def simular_comportamiento_humano_previo(page: Page) -> None:
    """Simula pequeños comportamientos humanos antes de acciones importantes"""
    async def _simular():
        # Ocasionalmente simular movimiento de mouse "distraído"
        if random.random() < 0.3:  # 30% de probabilidad
            await page.mouse.move(
                random.randint(200, 600),
                random.randint(150, 400)
            )
            await asyncio.sleep(random.uniform(0.2, 0.8))
    
    return _simular()

def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

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

    # User agent aleatorio para cada sesión
    selected_ua = random.choice(USER_AGENTS)
    
    context = await browser.new_context(
        locale="es-ES",
        user_agent=selected_ua,
        # Viewport ligeramente variable
        viewport={
            'width': random.randint(1280, 1440),
            'height': random.randint(720, 900)
        }
    )
    await context.add_cookies(cookies)
    return context

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    try:
        # Pequeña pausa humana antes de extraer
        await asyncio.sleep(random.uniform(0.3, 0.8))
        
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

async def scroll_hasta(page: Page, factor_fatiga: float = 0) -> bool:
    # Comportamiento humano previo ocasional
    if random.random() < 0.4:  # 40% probabilidad
        await simular_comportamiento_humano_previo(page)

    # Scroll más variable y humano
    scroll_amount = random.randint(120, 350)  # Cantidad más variable
    
    # Ocasionalmente hacer scroll "imperfecto" (como humano distraído)
    if random.random() < 0.15:  # 15% probabilidad
        scroll_amount = random.randint(50, 120)  # Scroll más pequeño

    prev = await page.evaluate("document.body.scrollHeight")

    # Scroll con posible componente horizontal mínimo (más humano)
    horizontal_drift = random.randint(-10, 10)
    await page.mouse.wheel(horizontal_drift, scroll_amount)
    
    # Delay con factor de fatiga
    delay = obtener_delay_humano(1.8, 3.2, factor_fatiga)
    await asyncio.sleep(delay)

    now = await page.evaluate("document.body.scrollHeight")
    return now > prev

async def procesar_lote_urls(page: Page, urls_lote: List[str], modelo: str, 
                           vistos_globales: Set[str], contador: Dict[str, int],
                           procesados: List[str], potenciales: List[str], 
                           relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]],
                           factor_fatiga: float = 0) -> int:
    """Procesa un lote de URLs con mejor gestión de errores y comportamiento más humano"""
    nuevos_en_lote = 0
    
    for i, url in enumerate(urls_lote):
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)

        try:
            # Timeout ligeramente aumentado para ser menos agresivo
            await asyncio.wait_for(page.goto(url), timeout=18)
            
            # Delay variable entre anuncios con factor de fatiga
            delay_anuncio = obtener_delay_humano(*DELAY_ENTRE_ANUNCIOS, factor_fatiga)
            await asyncio.sleep(delay_anuncio)
            
            # Extraer texto con timeout
            try:
                texto = await asyncio.wait_for(page.inner_text("div[role='main']"), timeout=12)
                if not texto or len(texto.strip()) < 100:
                    raise ValueError("Texto insuficiente")
            except Exception:
                # Fallback más rápido
                texto = await page.title() or "Sin texto disponible"

        except Exception as e:
            logger.warning(f"⚠️ Error al procesar {url}: {e}")
            continue

        # Procesamiento del anuncio (mantiene la lógica original)
        if not await procesar_anuncio_individual(page, url, texto, modelo, contador, 
                                         procesados, potenciales, relevantes, sin_anio_ejemplos):
            continue
            
        nuevos_en_lote += 1
        
        # Pausa adaptativa con más variabilidad
        if nuevos_en_lote % random.randint(2, 4) == 0:  # Cada 2-4 éxitos
            pausa = obtener_delay_humano(2.5, 4.5, factor_fatiga)
            await asyncio.sleep(pausa)

    return nuevos_en_lote

async def procesar_anuncio_individual(
    page: Page,
    url: str,
    texto: str,
    modelo: str,
    contador: Dict[str, int],
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str],
    sin_anio_ejemplos: List[Tuple[str, str]]
) -> bool:
    """Procesa un anuncio individual y retorna True si fue procesado exitosamente"""
    
    texto = texto.strip()
    if not coincide_modelo(texto, modelo):
        contador["filtro_modelo"] += 1
        return False
    if contiene_negativos(texto):
        contador["negativo"] += 1
        return False
    if "mexico" in texto.lower():
        contador["extranjero"] += 1
        return False

    m = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
    if not m:
        contador["sin_precio"] += 1
        return False
    precio = limpiar_precio(m.group())
    if precio < MIN_PRECIO_VALIDO:
        contador["precio_bajo"] += 1
        return False

    anio = extraer_anio(texto)

    # Intento expandir descripción con comportamiento más humano
    if not anio or not (1990 <= anio <= datetime.now().year):
        ver_mas = await page.query_selector("div[role='main'] span:has-text('Ver más')")
        if ver_mas:
            # Pequeña pausa antes del clic (más humano)
            await asyncio.sleep(random.uniform(0.5, 1.2))
            await ver_mas.click()
            
            # Espera variable para cargar contenido
            await asyncio.sleep(random.uniform(1.8, 2.5))
            texto_expandido = await page.inner_text("div[role='main']")
            anio_expandido = extraer_anio(texto_expandido)
            if anio_expandido and (1990 <= anio_expandido <= datetime.now().year):
                anio = anio_expandido
    
    # Segunda validación después del intento expandido
    if not anio or not (1990 <= anio <= datetime.now().year):
        contador["sin_anio"] += 1
        if len(sin_anio_ejemplos) < MAX_EJEMPLOS_SIN_ANIO:
            sin_anio_ejemplos.append((texto, url))
        return False

    roi_data = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio({
        "texto": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio,
        "roi": roi_data.get("roi", 0)
    })

    relevante = score >= SCORE_MIN_TELEGRAM and roi_data["roi"] >= ROI_MINIMO

    mensaje_base = (
        f"🚘 *{modelo.title()}*\n"
        f"• Año: {anio}\n"
        f"• Precio: Q{precio:,}\n"
        f"• ROI: {roi_data['roi']:.2f}%\n"
        f"• Score: {score}/10\n"
        f"🔗 {url}"
    )

    # Gestión de base de datos (mantiene lógica original)
    if existe_en_db(url):
        existente = obtener_anuncio_db(url)
        nuevo = {
            "modelo": modelo,
            "anio": anio,
            "precio": precio,
            "km": "",
            "roi": roi_data["roi"],
            "score": score
        }
        if anuncio_diferente(nuevo, existente):
            insertar_anuncio_db(link=url, modelo=modelo, anio=anio, precio=precio, km="", roi=roi_data["roi"],
                               score=score, relevante=relevante, confianza_precio=roi_data["confianza"],
                               muestra_precio=roi_data["muestra"])
            logger.info(f"🔄 Actualizado: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score}")
            contador["actualizados"] += 1
        else:
            contador["repetidos"] += 1
    else:
        insertar_anuncio_db(link=url, modelo=modelo, anio=anio, precio=precio, km="", roi=roi_data["roi"],
                           score=score, relevante=relevante, confianza_precio=roi_data["confianza"],
                           muestra_precio=roi_data["muestra"])
        logger.info(f"💾 Guardado nuevo: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score} | Relevante={relevante}")
        contador["guardado"] += 1

    procesados.append(mensaje_base)

    if relevante:
        relevantes.append(mensaje_base)
    elif ROI_POTENCIAL_MIN <= roi_data["roi"] < ROI_MINIMO:
        potenciales.append(mensaje_base)

    return True

async def procesar_ordenamiento_optimizado(page: Page, modelo: str, sort: str, 
                                         vistos_globales: Set[str], contador: Dict[str, int],
                                         procesados: List[str], potenciales: List[str], 
                                         relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]],
                                         modelo_index: int = 0) -> int:
    """Versión optimizada con comportamiento más humano"""
    
    url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
    await page.goto(url_busq)
    
    # Delay inicial variable
    await asyncio.sleep(obtener_delay_humano(MIN_DELAY, MAX_DELAY))

    # Scrolls con variabilidad
    max_scrolls = BASE_SCROLLS + random.randint(-SCROLL_VARIATION, SCROLL_VARIATION)
    scrolls_realizados = 0
    consec_repetidos = 0
    nuevos_total = 0
    urls_pendientes = []
    
    # Factor de fatiga basado en progreso
    factor_fatiga_base = modelo_index * 0.1  # Aumenta ligeramente con cada modelo

    while scrolls_realizados < max_scrolls:
        # Extraer URLs en lote
        items = await extraer_items_pagina(page)
        urls_nuevas = []
        
        for itm in items:
            url = limpiar_link(itm["url"])
            contador["total"] += 1

            if not url.startswith("https://www.facebook.com/marketplace/item/"):
                continue
                
            if url not in vistos_globales:
                urls_nuevas.append(url)

        urls_pendientes.extend(urls_nuevas)
        
        # Batch size variable
        batch_size = random.randint(*BATCH_SIZE_SCROLL)
        
        # Procesar en lotes cuando tengamos suficientes URLs o al final
        if len(urls_pendientes) >= batch_size or scrolls_realizados >= max_scrolls - 1:
            if urls_pendientes:
                lote_actual = urls_pendientes[:batch_size]
                urls_pendientes = urls_pendientes[batch_size:]
                
                factor_fatiga_actual = factor_fatiga_base + (scrolls_realizados / max_scrolls) * 0.2
                
                nuevos_en_lote = await procesar_lote_urls(
                    page, lote_actual, modelo, vistos_globales, 
                    contador, procesados, potenciales, relevantes, 
                    sin_anio_ejemplos, factor_fatiga_actual
                )
                nuevos_total += nuevos_en_lote
                
                if nuevos_en_lote == 0:
                    consec_repetidos += 1
                else:
                    consec_repetidos = 0

        scrolls_realizados += 1
        
        # Salida temprana optimizada
        if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS and len(urls_nuevas) < 2:
            logger.info(f"🔄 Salida temprana en {sort}: {consec_repetidos} scrolls consecutivos sin nuevos")
            break
            
        # Factor de fatiga para scrolls
        factor_fatiga_scroll = factor_fatiga_base + (scrolls_realizados / max_scrolls) * 0.15
        if not await scroll_hasta(page, factor_fatiga_scroll):
            logger.info(f"🔄 Fin de contenido detectado en {sort}")
            break

    # Procesar URLs restantes
    if urls_pendientes:
        await procesar_lote_urls(
            page, urls_pendientes, modelo, vistos_globales, 
            contador, procesados, potenciales, relevantes, sin_anio_ejemplos
        )

    return nuevos_total

async def procesar_modelo(page: Page, modelo: str,
                          procesados: List[str],
                          potenciales: List[str],
                          relevantes: List[str],
                          modelo_index: int = 0) -> int:
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos"
    ]}
    
    # Orden aleatorio de sorts para cada modelo
    SORT_OPTS = ["best_match", "price_asc"]
    if random.random() < 0.3:  # 30% probabilidad de incluir newest ocasionalmente
        SORT_OPTS.append("newest")
    random.shuffle(SORT_OPTS)
    
    inicio = datetime.now()
    total_nuevos = 0

    for i, sort in enumerate(SORT_OPTS):
        logger.info(f"🔍 Procesando {modelo} con ordenamiento: {sort}")
        try:
            nuevos_sort = await asyncio.wait_for(
                procesar_ordenamiento_optimizado(
                    page, modelo, sort, vistos_globales, contador,
                    procesados, potenciales, relevantes, sin_anio_ejemplos, modelo_index
                ), 
                timeout=200  # Timeout ligeramente aumentado
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos anuncios procesados")
            
            # Pausa variable entre ordenamientos
            if i < len(SORT_OPTS) - 1:  # No pausar después del último
                pausa_sort = obtener_delay_humano(4.0, 8.0, modelo_index * 0.1)
                await asyncio.sleep(pausa_sort)
            
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout en ordenamiento {sort} para {modelo}")
            continue

    duracion = (datetime.now() - inicio).seconds
    logger.info(f"""
✨ MODELO: {modelo.upper()} - MEJORADO
   Duración: {duracion} s
   Total encontrados: {contador['total']}
   Guardados nuevos: {contador['guardado']}
   Actualizados: {contador.get('actualizados', 0)}
   Repetidos sin cambios: {contador.get('repetidos', 0)}
   Relevantes: {len([r for r in relevantes if modelo.lower() in r.lower()])}
   Potenciales: {len([p for p in potenciales if modelo.lower() in p.lower()])}
   Duplicados: {contador['duplicado']}
   Desc. por score/modelo: {contador['filtro_modelo']}
   Precio bajo: {contador['precio_bajo']}
   Sin año: {contador['sin_anio']}
   Negativos: {contador['negativo']}
   Extranjero: {contador['extranjero']}
   ✨""")

    return total_nuevos

async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    inicializar_tabla_anuncios()
    modelos = modelos_override or MODELOS_INTERES
    flops = modelos_bajo_rendimiento()
    activos = [m for m in modelos if m not in flops]

    procesados, potenciales, relevantes = [], [], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox', 
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',  # Anti-detección
                '--disable-features=VizDisplayCompositor'  # Reduce carga GPU
            ]
        )
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        # Headers más realistas
        await page.set_extra_http_headers({
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        })

        await page.goto("https://www.facebook.com/marketplace")
        await asyncio.sleep(obtener_delay_humano(3.0, 5.0))

        if "login" in page.url or "recover" in page.url:
            alerta = "🚨 Sesión inválida: redirigido a la página de inicio de sesión. Verifica las cookies (FB_COOKIES_JSON)."
            logger.warning(alerta)
            return [], [], [alerta]

        logger.info("✅ Sesión activa detectada correctamente en Marketplace.")

        # Aleatorizar pero con seed para reproducibilidad si es necesario
        modelos_shuffled = activos.copy()
        random.shuffle(modelos_shuffled)

        for i, m in enumerate(modelos_shuffled):
            logger.info(f"📋 Procesando modelo {i+1}/{len(modelos_shuffled)}: {m}")
            try:
                await asyncio.wait_for(
                    procesar_modelo(page, m, procesados, potenciales, relevantes, i), 
                    timeout=420  # 7 minutos por modelo (ligeramente aumentado)
                )
                
                # Pausa variable entre modelos con factor de fatiga
                if i < len(modelos_shuffled) - 1:
                    pausa_modelo = obtener_delay_humano(10.0, 20.0, i * 0.05)
                    await asyncio.sleep(pausa_modelo)
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏳ {m} → Excedió tiempo máximo. Se aborta.")

        await browser.close()

    return procesados, potenciales, relevantes

if __name__ == "__main__":
    async def main():
        procesados, potenciales, relevantes = await buscar_autos_marketplace()

        logger.info("📦 Resumen final del scraping mejorado")
        logger.info(f"Guardados totales: {len(procesados)}")
        logger.info(f"Relevantes: {len(relevantes)}")
        logger.info(f"Potenciales: {len(potenciales)}")

        logger.info("\n🟢 Relevantes con buen ROI:")
        for r in relevantes[:10]:
            logger.info(r.replace("*", "").replace("\\n", "\n"))

    asyncio.run(main())
