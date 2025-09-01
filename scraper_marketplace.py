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
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
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

# Configuración optimizada y menos detectable
MAX_SCROLLS_POR_SORT = 12  # Más conservador
MIN_DELAY = 2.5  # Aumentado para parecer más humano
MAX_DELAY = 5.5  # Aumentado para parecer más humano
DELAY_ENTRE_ANUNCIOS = 2.8  # Ligeramente aumentado
MAX_CONSECUTIVOS_SIN_NUEVOS = 3
BATCH_SIZE_SCROLL = 6  # Reducido para ser menos agresivo

# Pool de User Agents realistas
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0"
]

def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    logger.info("🔐 Cargando cookies desde entorno…")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    
    # Seleccionar User Agent aleatorio para esta sesión
    user_agent = random.choice(USER_AGENTS)
    
    # Configuración base más realista
    context_config = {
        "locale": "es-ES",
        "user_agent": user_agent,  # 👈 Siempre establecer user_agent
        "viewport": {"width": random.randint(1366, 1920), "height": random.randint(768, 1080)},
        "extra_http_headers": {
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1'
        }
    }
    
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima con UA consistente.")
        return await browser.new_context(**context_config)
        
    try:
        cookies = json.loads(cj)
        context = await browser.new_context(**context_config)
        await context.add_cookies(cookies)
        logger.info(f"✅ Cookies cargadas con UA: {user_agent[:50]}...")
        return context
    except Exception as e:
        logger.error(f"❌ Error al parsear FB_COOKIES_JSON: {e}")
        return await browser.new_context(**context_config)

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extracción más robusta usando aria-label cuando esté disponible"""
    try:
        # Micro-pausa para parecer más humano
        await asyncio.sleep(random.uniform(0.2, 0.6))
        
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        
        for a in items:
            # Priorizar aria-label que suele tener texto completo
            aria_label = await a.get_attribute("aria-label") or ""
            titulo = (await a.inner_text()).strip() if not aria_label else ""
            texto_completo = (aria_label or titulo).strip()
            
            href = await a.get_attribute("href") or ""
            if texto_completo and href:
                resultados.append({"texto": texto_completo, "url": limpiar_url(href)})
        
        return resultados
    except Exception as e:
        logger.error(f"❌ Error al extraer items: {e}")
        return []

async def scroll_hasta(page: Page) -> bool:
    """Scroll más humano con variación de técnicas"""
    # Simular movimiento de mouse más realista
    await page.mouse.move(
        random.randint(200, 800),
        random.randint(200, 600),
        steps=random.randint(3, 8)  # 👈 Movimiento gradual
    )
    await asyncio.sleep(random.uniform(0.3, 0.8))

    # Evaluar altura inicial
    prev = await page.evaluate("document.body.scrollHeight")

    # Alternar entre técnicas de scroll para ser menos predecible
    if random.random() < 0.7:  # 70% wheel, 30% keyboard/JS
        # Scroll con wheel (más natural)
        scroll_amount = random.randint(200, 450)
        await page.mouse.wheel(0, scroll_amount)
    else:
        # Scroll con JavaScript (alternativa menos común)
        scroll_amount = random.randint(300, 600)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
    
    # Delay más humano y variable
    await asyncio.sleep(random.uniform(2.0, 3.5))

    # Evaluar nueva altura
    now = await page.evaluate("document.body.scrollHeight")
    return now > prev

async def esperar_carga_anuncio(page: Page, max_attempts: int = 3) -> bool:
    """Espera explícita a que cargue el contenido del anuncio"""
    for attempt in range(max_attempts):
        try:
            # Esperar que aparezca el contenido principal
            await page.wait_for_selector("div[role='main']", timeout=8000)
            
            # Pequeña pausa adicional para asegurar carga completa
            await asyncio.sleep(random.uniform(0.5, 1.2))
            return True
            
        except PlaywrightTimeoutError:
            if attempt < max_attempts - 1:
                logger.debug(f"🔄 Intento {attempt + 1} fallido, reintentando...")
                await asyncio.sleep(1)
            continue
    
    logger.warning("⚠️ No se pudo cargar completamente el anuncio")
    return False

async def procesar_lote_urls(page: Page, urls_lote: List[str], modelo: str, 
                           vistos_globales: Set[str], contador: Dict[str, int],
                           procesados: List[str], potenciales: List[str], 
                           relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Procesa un lote de URLs con navegación más robusta"""
    nuevos_en_lote = 0
    
    for i, url in enumerate(urls_lote):
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)

        try:
            # Navegación más robusta con espera de carga
            await asyncio.wait_for(
                page.goto(url, wait_until="domcontentloaded"),  # 👈 Esperar DOM
                timeout=18
            )
            
            # Esperar carga específica del anuncio
            if not await esperar_carga_anuncio(page):
                logger.debug(f"⚠️ Anuncio no cargó completamente: {url}")
                continue
            
            # Delay entre anuncios con variación
            base_delay = DELAY_ENTRE_ANUNCIOS
            if i > 0 and i % 3 == 0:  # Pausa extra cada 3 anuncios
                base_delay *= 1.5
            await asyncio.sleep(random.uniform(base_delay * 0.8, base_delay * 1.3))
            
            # Extracción de texto con fallback mejorado
            try:
                # Intentar selector más específico primero
                descripcion_selector = "div[role='main'] div[data-pagelet='MarketplaceProductDetailsPageLayout'] div"
                try:
                    texto = await asyncio.wait_for(
                        page.inner_text(descripcion_selector), timeout=8
                    )
                except:
                    # Fallback al selector general
                    texto = await asyncio.wait_for(
                        page.inner_text("div[role='main']"), timeout=8
                    )
                
                if not texto or len(texto.strip()) < 50:
                    raise ValueError("Texto insuficiente para análisis")
                    
            except Exception:
                # Último recurso: título de la página
                texto = await page.title() or "Sin texto disponible"
                if len(texto) < 10:
                    continue

        except (PlaywrightTimeoutError, asyncio.TimeoutError) as e:
            logger.warning(f"⏳ Timeout al procesar {url}: {e}")
            continue
        except Exception as e:
            logger.warning(f"⚠️ Error inesperado al procesar {url}: {e}")
            continue

        # Procesamiento del anuncio (mantiene la lógica original)
        if not await procesar_anuncio_individual(page, url, texto, modelo, contador, 
                                         procesados, potenciales, relevantes, sin_anio_ejemplos):
            continue
            
        nuevos_en_lote += 1
        
        # Micro-pausa adicional aleatoria
        await asyncio.sleep(random.uniform(0.1, 0.4))

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
    """Procesa un anuncio individual con manejo mejorado del botón 'Ver más'"""
    
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

    # Expansión de descripción más robusta si no hay año válido
    if not anio or not (1990 <= anio <= datetime.now().year):
        # Selectores múltiples para "Ver más" - 👈 Mejorado
        ver_mas_selectores = [
            "span:has-text('Ver más')",
            "div[role='button']:has-text('Ver más')",
            "span[role='button']:has-text('Ver más')",
            "div:has-text('Ver más')"
        ]
        
        ver_mas = None
        for selector in ver_mas_selectores:
            try:
                ver_mas = await page.query_selector(f"div[role='main'] {selector}")
                if ver_mas:
                    break
            except:
                continue
        
        if ver_mas:
            try:
                # Scroll al botón si no está visible
                await ver_mas.scroll_into_view_if_needed()
                await asyncio.sleep(random.uniform(0.3, 0.7))
                
                await ver_mas.click()
                await asyncio.sleep(random.uniform(1.2, 2.0))  # Tiempo para carga
                
                texto_expandido = await page.inner_text("div[role='main']")
                anio_expandido = extraer_anio(texto_expandido)
                
                if anio_expandido and (1990 <= anio_expandido <= datetime.now().year):
                    anio = anio_expandido
                    
            except Exception as e:
                logger.debug(f"⚠️ No se pudo expandir descripción: {e}")
    
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
                                         relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Versión mejorada con navegación más robusta"""
    
    url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
    
    try:
        await page.goto(url_busq, wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        # Verificar que estamos en la página correcta
        if "login" in page.url or "recover" in page.url:
            logger.warning(f"🚨 Redirigido a login durante búsqueda de {modelo}")
            return 0
            
    except Exception as e:
        logger.error(f"❌ Error al navegar a búsqueda de {modelo}: {e}")
        return 0

    scrolls_realizados = 0
    consec_repetidos = 0
    nuevos_total = 0
    urls_pendientes = []

    while scrolls_realizados < MAX_SCROLLS_POR_SORT:
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
        
        # Procesar en lotes más pequeños para ser menos agresivo
        if len(urls_pendientes) >= BATCH_SIZE_SCROLL or scrolls_realizados >= MAX_SCROLLS_POR_SORT - 1:
            if urls_pendientes:
                lote_actual = urls_pendientes[:BATCH_SIZE_SCROLL]
                urls_pendientes = urls_pendientes[BATCH_SIZE_SCROLL:]
                
                nuevos_en_lote = await procesar_lote_urls(page, lote_actual, modelo, vistos_globales, 
                                                        contador, procesados, potenciales, relevantes, sin_anio_ejemplos)
                nuevos_total += nuevos_en_lote
                
                if nuevos_en_lote == 0:
                    consec_repetidos += 1
                else:
                    consec_repetidos = 0
                
                # Pausa adicional después de procesar lote
                if urls_pendientes:  # Si quedan más por procesar
                    await asyncio.sleep(random.uniform(3.0, 6.0))

        scrolls_realizados += 1
        
        # Salida temprana más conservadora
        if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS and len(urls_nuevas) < 2:
            logger.info(f"🔄 Salida temprana en {sort}: {consec_repetidos} scrolls consecutivos sin nuevos")
            break
            
        if not await scroll_hasta(page):
            logger.info(f"🔄 Fin de contenido detectado en {sort}")
            break
        
        # Micro-pausa entre scrolls
        await asyncio.sleep(random.uniform(0.3, 0.8))

    # Procesar URLs restantes
    if urls_pendientes:
        await procesar_lote_urls(page, urls_pendientes, modelo, vistos_globales, 
                               contador, procesados, potenciales, relevantes, sin_anio_ejemplos)

    return nuevos_total

async def procesar_modelo(page: Page, modelo: str,
                          procesados: List[str],
                          potenciales: List[str],
                          relevantes: List[str]) -> int:
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos"
    ]}
    
    # Ordenamientos con rotación para ser menos predecible
    SORT_OPTS = ["best_match", "price_asc"]
    if random.random() < 0.3:  # 30% de las veces incluir creation_time_desc
        SORT_OPTS.append("creation_time_desc")
    
    # Aleatorizar orden para ser menos predecible
    random.shuffle(SORT_OPTS)
    
    inicio = datetime.now()
    total_nuevos = 0

    for sort in SORT_OPTS:
        logger.info(f"🔍 Procesando {modelo} con ordenamiento: {sort}")
        try:
            nuevos_sort = await asyncio.wait_for(
                procesar_ordenamiento_optimizado(page, modelo, sort, vistos_globales, contador,
                                                procesados, potenciales, relevantes, sin_anio_ejemplos), 
                timeout=200  # Timeout más generoso
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos anuncios procesados")
            
            # Pausa variable entre ordenamientos
            if SORT_OPTS.index(sort) < len(SORT_OPTS) - 1:  # No pausar después del último
                pausa = random.uniform(5.0, 10.0)
                await asyncio.sleep(pausa)
            
        except (asyncio.TimeoutError, PlaywrightTimeoutError):
            logger.warning(f"⏳ Timeout en ordenamiento {sort} para {modelo}")
            continue
        except Exception as e:
            logger.error(f"❌ Error inesperado en {sort} para {modelo}: {e}")
            continue

    duracion = (datetime.now() - inicio).seconds
    logger.info(f"""
✨ MODELO: {modelo.upper()} - MODO STEALTH
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

    # Configuración de navegador más realista
    headless_mode = random.choice([True, True, True, False])  # 75% headless, 25% visible
    
    browser_args = [
        '--no-sandbox', 
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=VizDisplayCompositor',
        '--window-size=1366,768'  # Tamaño común
    ]
    
    if headless_mode:
        browser_args.extend([
            '--disable-gpu',
            '--no-first-run',
            '--disable-default-apps'
        ])

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless_mode,
            args=browser_args
        )
        
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        # Configuración adicional anti-detección
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Ocultar que es automatización
            window.chrome = {
                runtime: {},
            };
            
            // Simular plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
        """)

        # Navegación inicial más robusta
        try:
            await page.goto("https://www.facebook.com/marketplace", wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3.0, 5.0))

            if "login" in page.url or "recover" in page.url:
                alerta = "🚨 Sesión inválida: redirigido a la página de inicio de sesión. Verifica las cookies (FB_COOKIES_JSON)."
                logger.warning(alerta)
                return [], [], [alerta]

            logger.info("✅ Sesión activa detectada correctamente en Marketplace.")

        except Exception as e:
            logger.error(f"❌ Error al acceder a Marketplace: {e}")
            return [], [], [f"Error de conexión: {e}"]

        # Procesamiento de modelos con orden aleatorio
        modelos_shuffled = activos.copy()
        random.shuffle(modelos_shuffled)

        for i, m in enumerate(modelos_shuffled):
            logger.info(f"📋 Procesando modelo {i+1}/{len(modelos_shuffled)}: {m}")
            try:
                await asyncio.wait_for(
                    procesar_modelo(page, m, procesados, potenciales, relevantes), 
                    timeout=400  # Timeout más generoso por modelo
                )
                
                # Pausa variable y más larga entre modelos
                if i < len(modelos_shuffled) - 1:
                    pausa_entre_modelos = random.uniform(12.0, 25.0)
                    logger.info(f"⏸️ Pausa de {pausa_entre_modelos:.1f}s antes del siguiente modelo")
                    await asyncio.sleep(pausa_entre_modelos)
                    
            except (asyncio.TimeoutError, PlaywrightTimeoutError):
                logger.warning(f"⏳ {m} → Excedió tiempo máximo. Se aborta.")
            except Exception as e:
                logger.error(f"❌ Error inesperado procesando {m}: {e}")

        await browser.close()

    return procesados, potenciales, relevantes

if __name__ == "__main__":
    async def main():
        procesados, potenciales, relevantes = await buscar_autos_marketplace()

        logger.info("📦 Resumen final del scraping stealth")
        logger.info(f"Guardados totales: {len(procesados)}")
        logger.info(f"Relevantes: {len(relevantes)}")
        logger.info(f"Potenciales: {len(potenciales)}")

        logger.info("\n🟢 Relevantes con buen ROI:")
        for r in relevantes[:10]:
            logger.info(r.replace("*", "").replace("\\n", "\n"))

    asyncio.run(main())
