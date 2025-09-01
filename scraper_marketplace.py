# scraper_marketplace_mejorado.py

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

# Configuración base
MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5
ROI_POTENCIAL_MIN = ROI_MINIMO - 10

# Configuración antidetección optimizada
MAX_SCROLLS_POR_SORT = 12
MIN_DELAY = 1.2
MAX_DELAY = 3.8
DELAY_ENTRE_ANUNCIOS = 1.8
MAX_CONSECUTIVOS_SIN_NUEVOS = 3
BATCH_SIZE_SCROLL = 6
MAX_RETRIES = 3

# User agents modernos y realistas
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0"
]

# Viewports realistas
VIEWPORTS = [
    {"width": 1280, "height": 720},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080}
]

def generar_configuracion_navegador():
    """Genera configuración aleatoria para el navegador"""
    return {
        "user_agent": random.choice(USER_AGENTS),
        "viewport": random.choice(VIEWPORTS),
        "headless": random.choice([True, True, False]),  # 2/3 headless, 1/3 visible
        "locale": random.choice(["es-ES", "es-LA", "es-GT"])
    }

def limpiar_url(link: str) -> str:
    """Limpia y normaliza URLs de Facebook Marketplace"""
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def guardar_cookies_inteligente(context: BrowserContext, forzar: bool = False):
    """Guarda cookies solo si han pasado más de 10 minutos desde la última vez"""
    archivo_cookies = "fb_cookies.json"
    
    if not forzar:
        try:
            # Verificar si el archivo existe y cuándo se modificó por última vez
            if os.path.exists(archivo_cookies):
                tiempo_modificacion = os.path.getmtime(archivo_cookies)
                tiempo_actual = datetime.now().timestamp()
                diferencia_minutos = (tiempo_actual - tiempo_modificacion) / 60
                
                if diferencia_minutos < 10:  # Menos de 10 minutos
                    logger.debug(f"🍪 Cookies guardadas hace {diferencia_minutos:.1f}min, saltando...")
                    return
        except Exception:
            pass  # Si hay error, proceder a guardar
    
    try:
        cookies = await context.cookies()
        with open(archivo_cookies, 'w') as f:
            json.dump(cookies, f)
        logger.info(f"🍪 Cookies {'forzadas' if forzar else 'actualizadas'}")
    except Exception as e:
        logger.warning(f"⚠️ Error al guardar cookies: {e}")

async def cargar_contexto_con_cookies(browser: Browser, config: Dict) -> BrowserContext:
    """Carga contexto del navegador con cookies y configuración antidetección"""
    logger.info("🔐 Configurando contexto del navegador...")
    
    # Cargar cookies desde variable de entorno (GitHub Secrets)
    cj = os.environ.get("FB_COOKIES_JSON", "")
    cookies = []
    
    if cj:
        try:
            cookies = json.loads(cj)
            logger.info("✅ Cookies cargadas desde GitHub Secret")
        except Exception as e:
            logger.warning(f"⚠️ Error al parsear FB_COOKIES_JSON: {e}")
    else:
        logger.info("ℹ️ No se encontraron cookies en variables de entorno")

    # Configuración avanzada del contexto
    context = await browser.new_context(
        user_agent=config["user_agent"],
        viewport=config["viewport"],
        locale=config["locale"],
        timezone_id="America/Guatemala",
        geolocation={"latitude": 14.6349, "longitude": -90.5069},  # Guatemala City
        permissions=["geolocation"],
        extra_http_headers={
            "Accept-Language": f"{config['locale']},es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Upgrade-Insecure-Requests": "1"
        }
    )
    
    if cookies:
        await context.add_cookies(cookies)
    
    return context

async def accion_distraccion(page: Page):
    """Realiza acciones de distracción para parecer más humano"""
    acciones = [
        lambda: page.mouse.move(random.randint(100, 800), random.randint(100, 600)),
        lambda: page.mouse.wheel(0, random.randint(-50, 50)),
        lambda: page.keyboard.press("Tab"),
        lambda: asyncio.sleep(random.uniform(0.3, 0.8))
    ]
    
    accion = random.choice(acciones)
    try:
        await accion()
    except Exception:
        pass  # Ignorar errores en acciones de distracción

async def scroll_humano(page: Page) -> bool:
    """Simula scroll humano más realista"""
    # Movimiento de mouse previo
    await page.mouse.move(
        random.randint(200, 1000),
        random.randint(150, 700)
    )
    await asyncio.sleep(random.uniform(0.2, 0.6))
    
    # Altura inicial
    prev_height = await page.evaluate("document.body.scrollHeight")
    
    # Alternar entre métodos de scroll
    scroll_method = random.choice(["wheel", "evaluate", "keyboard"])
    
    if scroll_method == "wheel":
        # Scroll con rueda del mouse
        scroll_distance = random.randint(150, 400)
        await page.mouse.wheel(0, scroll_distance)
    elif scroll_method == "evaluate":
        # Scroll con JavaScript
        scroll_distance = random.randint(200, 500)
        await page.evaluate(f"window.scrollBy(0, {scroll_distance})")
    else:
        # Scroll con teclado
        await page.keyboard.press("PageDown")
    
    # Pausa irregular
    await asyncio.sleep(random.uniform(0.8, 2.2))
    
    # Verificar cambio de altura
    new_height = await page.evaluate("document.body.scrollHeight")
    return new_height > prev_height

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extrae items de la página con selectores robustos"""
    selectores = [
        "a[href*='/marketplace/item']",
        "div[role='article'] a[href*='/marketplace/item']",
        "[data-testid='marketplace-item'] a"
    ]
    
    items = []
    for selector in selectores:
        try:
            elementos = await page.query_selector_all(selector)
            if elementos:
                for elem in elementos:
                    try:
                        titulo = (await elem.inner_text()).strip()
                        aria_label = await elem.get_attribute("aria-label") or ""
                        texto_completo = f"{titulo} {aria_label}".strip()
                        href = await elem.get_attribute("href") or ""
                        
                        if texto_completo and href:
                            items.append({
                                "texto": texto_completo,
                                "url": limpiar_url(href)
                            })
                    except Exception:
                        continue
                break  # Usar el primer selector que funcione
        except Exception:
            continue
    
    return items

async def expandir_descripcion(page: Page) -> str:
    """Expande la descripción completa del anuncio"""
    selectores_ver_mas = [
        "span:has-text('Ver más')",
        "div[role='button']:has-text('Ver más')",
        "[data-testid='see-more-button']",
        "span:has-text('See more')"
    ]
    
    for selector in selectores_ver_mas:
        try:
            boton = await page.query_selector(selector)
            if boton and await boton.is_visible():
                await boton.click()
                await asyncio.sleep(random.uniform(1.2, 2.0))
                logger.debug("✅ Descripción expandida")
                break
        except Exception:
            continue
    
    # Extraer texto con selectores robustos
    selectores_contenido = [
        "div[role='main']",
        "[data-testid='marketplace-item-description']",
        "div[data-testid='post_message']"
    ]
    
    texto_completo = ""
    for selector in selectores_contenido:
        try:
            elemento = await page.query_selector(selector)
            if elemento:
                texto = await elemento.inner_text()
                if texto and len(texto.strip()) > len(texto_completo):
                    texto_completo = texto
        except Exception:
            continue
    
    # Limpiar y limitar texto (primeras 30 líneas)
    if texto_completo:
        lineas = texto_completo.strip().split('\n')
        texto_limitado = '\n'.join(lineas[:30])
        return texto_limitado
    
    # Fallback al título de la página
    try:
        return await page.title() or "Sin descripción disponible"
    except Exception:
        return "Sin descripción disponible"

async def procesar_anuncio_con_reintentos(page: Page, url: str, max_retries: int = MAX_RETRIES) -> Optional[str]:
    """Procesa un anuncio individual con sistema de reintentos"""
    for intento in range(max_retries):
        try:
            # Timeout progresivo
            timeout = 10 + (intento * 5)
            await asyncio.wait_for(page.goto(url), timeout=timeout)
            
            # Pausa humana después de cargar
            await asyncio.sleep(random.uniform(1.5, 2.8))
            
            # Expandir descripción y extraer texto
            texto = await expandir_descripcion(page)
            
            if texto and len(texto.strip()) > 50:
                return texto
            else:
                raise ValueError("Texto insuficiente extraído")
                
        except Exception as e:
            logger.warning(f"⚠️ Intento {intento + 1}/{max_retries} falló para {url}: {e}")
            if intento < max_retries - 1:
                await asyncio.sleep(random.uniform(2.0, 4.0))
            else:
                logger.error(f"❌ Falló definitivamente: {url}")
                return None
    
    return None

async def procesar_lote_urls_mejorado(page: Page, urls_lote: List[str], modelo: str, 
                                    vistos_globales: Set[str], contador: Dict[str, int],
                                    procesados: List[str], potenciales: List[str], 
                                    relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Procesa lote de URLs con mejoras de estabilidad y antidetección"""
    nuevos_en_lote = 0
    
    # Mezclar URLs para variar el orden
    urls_mezcladas = urls_lote.copy()
    random.shuffle(urls_mezcladas)
    
    for i, url in enumerate(urls_mezcladas):
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)
        
        # Acción de distracción ocasional
        if random.random() < 0.15:  # 15% de probabilidad
            await accion_distraccion(page)
        
        # Procesar anuncio con reintentos
        texto_anuncio = await procesar_anuncio_con_reintentos(page, url)
        if not texto_anuncio:
            continue
        
        # Procesamiento del anuncio (mantiene lógica original)
        if not await procesar_anuncio_individual(page, url, texto_anuncio, modelo, contador, 
                                               procesados, potenciales, relevantes, sin_anio_ejemplos):
            continue
        
        nuevos_en_lote += 1
        
        # Pausa adaptativa y humana
        if i < len(urls_mezcladas) - 1:  # No pausar en el último
            pausa = random.uniform(DELAY_ENTRE_ANUNCIOS * 0.8, DELAY_ENTRE_ANUNCIOS * 1.5)
            await asyncio.sleep(pausa)
        
        # Pausa extra cada ciertos anuncios
        if (i + 1) % 4 == 0:
            await asyncio.sleep(random.uniform(3.0, 6.0))
    
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
    """Procesa anuncio individual - mantiene lógica original"""
    
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
    
    # Validación de año
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

async def procesar_ordenamiento_antideteccion(page: Page, modelo: str, sort: str, 
                                            vistos_globales: Set[str], contador: Dict[str, int],
                                            procesados: List[str], potenciales: List[str], 
                                            relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Procesamiento por ordenamiento con antidetección mejorada"""
    
    url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
    
    # Cargar página con reintentos
    for intento in range(MAX_RETRIES):
        try:
            await page.goto(url_busq)
            break
        except Exception as e:
            logger.warning(f"⚠️ Error cargando búsqueda (intento {intento + 1}): {e}")
            if intento < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(3.0, 6.0))
    
    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

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
        
        # Procesar en lotes cuando tengamos suficientes URLs
        if len(urls_pendientes) >= BATCH_SIZE_SCROLL or scrolls_realizados >= MAX_SCROLLS_POR_SORT - 1:
            if urls_pendientes:
                lote_actual = urls_pendientes[:BATCH_SIZE_SCROLL]
                urls_pendientes = urls_pendientes[BATCH_SIZE_SCROLL:]
                
                nuevos_en_lote = await procesar_lote_urls_mejorado(page, lote_actual, modelo, vistos_globales, 
                                                                 contador, procesados, potenciales, relevantes, sin_anio_ejemplos)
                nuevos_total += nuevos_en_lote
                
                if nuevos_en_lote == 0:
                    consec_repetidos += 1
                else:
                    consec_repetidos = 0

        scrolls_realizados += 1
        
        # Salida temprana optimizada
        if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS and len(urls_nuevas) < 2:
            logger.info(f"🔄 Salida temprana en {sort}: {consec_repetidos} scrolls sin nuevos")
            break
            
        # Scroll humano
        if not await scroll_humano(page):
            logger.info(f"🔄 Fin de contenido detectado en {sort}")
            break

    # Procesar URLs restantes
    if urls_pendientes:
        await procesar_lote_urls_mejorado(page, urls_pendientes, modelo, vistos_globales, 
                                        contador, procesados, potenciales, relevantes, sin_anio_ejemplos)

    return nuevos_total

async def procesar_modelo(page: Page, modelo: str, context: BrowserContext,
                          procesados: List[str],
                          potenciales: List[str],
                          relevantes: List[str]) -> int:
    """Procesa un modelo específico con todas las mejoras integradas"""
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos"
    ]}
    
    # Ordenamientos con mejor rendimiento
    SORT_OPTS = ["best_match", "price_asc"]
    random.shuffle(SORT_OPTS)  # Aleatorizar orden
    
    inicio = datetime.now()
    total_nuevos = 0

    for i, sort in enumerate(SORT_OPTS):
        logger.info(f"🔍 Procesando {modelo} con ordenamiento: {sort}")
        try:
            nuevos_sort = await asyncio.wait_for(
                procesar_ordenamiento_antideteccion(page, modelo, sort, vistos_globales, contador,
                                                   procesados, potenciales, relevantes, sin_anio_ejemplos), 
                timeout=240  # 4 minutos por ordenamiento
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos anuncios procesados")
            
            # Pausa entre ordenamientos con variación
            if i < len(SORT_OPTS) - 1:
                pausa = random.uniform(5.0, 12.0)
                logger.info(f"⏳ Pausa de {pausa:.1f}s entre ordenamientos...")
                await asyncio.sleep(pausa)
            
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout en ordenamiento {sort} para {modelo}")
            continue
        except Exception as e:
            logger.error(f"❌ Error en ordenamiento {sort}: {e}")
            continue

    # Guardar cookies después de procesar cada modelo
    await guardar_cookies(context)

    duracion = (datetime.now() - inicio).seconds
    logger.info(f"""
✨ MODELO: {modelo.upper()} - ANTIDETECCIÓN MEJORADA
   Duración: {duracion}s
   Total encontrados: {contador['total']}
   Guardados nuevos: {contador['guardado']}
   Actualizados: {contador.get('actualizados', 0)}
   Repetidos sin cambios: {contador.get('repetidos', 0)}
   Relevantes: {len([r for r in relevantes if modelo.lower() in r.lower()])}
   Potenciales: {len([p for p in potenciales if modelo.lower() in p.lower()])}
   Duplicados: {contador['duplicado']}
   Filtrados: {contador['filtro_modelo']}
   Precio bajo: {contador['precio_bajo']}
   Sin año: {contador['sin_anio']}
   Negativos: {contador['negativo']}
   Extranjero: {contador['extranjero']}
   ✨""")

    return total_nuevos

async def buscar_autos_marketplace_mejorado(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Función principal mejorada con antidetección y estabilidad"""
    inicializar_tabla_anuncios()
    modelos = modelos_override or MODELOS_INTERES
    flops = modelos_bajo_rendimiento()
    activos = [m for m in modelos if m not in flops]

    procesados, potenciales, relevantes = [], [], []
    
    # Generar configuración aleatoria del navegador
    config = generar_configuracion_navegador()
    logger.info(f"🔧 Configuración: {config['user_agent'][:50]}... | Headless: {config['headless']} | Viewport: {config['viewport']}")

    async with async_playwright() as p:
        # Configuración del navegador con antidetección
        browser = await p.chromium.launch(
            headless=config["headless"],
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--no-first-run',
                '--no-default-browser-check',
                '--disable-infobars',
                '--disable-extensions',
                '--start-maximized'
            ]
        )
        
        context = await cargar_contexto_con_cookies(browser, config)
        page = await context.new_page()

        # Ocultar webdriver
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            window.chrome = {
                runtime: {},
            };
            
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            Object.defineProperty(navigator, 'languages', {
                get: () => ['es-ES', 'es', 'en'],
            });
        """)

        # Ir a Marketplace con verificación de sesión
        try:
            await page.goto("https://www.facebook.com/marketplace")
            await asyncio.sleep(random.uniform(4.0, 7.0))

            if "login" in page.url or "recover" in page.url:
                alerta = "🚨 Sesión inválida: redirigido a login. Verifica cookies (FB_COOKIES_JSON)."
                logger.warning(alerta)
                await browser.close()
                return [], [], [alerta]

            logger.info("✅ Sesión activa detectada correctamente en Marketplace.")
        except Exception as e:
            logger.error(f"❌ Error inicial: {e}")
            await browser.close()
            return [], [], [f"Error de conexión: {e}"]

        # Procesar modelos con orden aleatorizado
        modelos_shuffled = activos.copy()
        random.shuffle(modelos_shuffled)

        for i, modelo in enumerate(modelos_shuffled):
            logger.info(f"📋 Procesando modelo {i+1}/{len(modelos_shuffled)}: {modelo}")
            try:
                await asyncio.wait_for(
                    procesar_modelo(page, modelo, context, procesados, potenciales, relevantes), 
                    timeout=600  # 10 minutos por modelo
                )
                
                # Pausa entre modelos con variación humana
                if i < len(modelos_shuffled) - 1:
                    pausa = random.uniform(10.0, 25.0)
                    logger.info(f"⏳ Pausa de {pausa:.1f}s entre modelos...")
                    await asyncio.sleep(pausa)
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏳ {modelo} → Excedió tiempo máximo (10min)")
            except Exception as e:
                logger.error(f"❌ Error procesando {modelo}: {e}")

        # Procesamiento terminado - no guardamos cookies en GitHub Actions
        await browser.close()

    return procesados, potenciales, relevantes

# Mantener compatibilidad con nombre original
async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Función wrapper para mantener compatibilidad"""
    return await buscar_autos_marketplace_mejorado(modelos_override)

if __name__ == "__main__":
    async def main():
        logger.info("🚀 Iniciando scraper mejorado con antidetección...")
        procesados, potenciales, relevantes = await buscar_autos_marketplace_mejorado()

        # Guardar resultados en archivo JSON con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        resultados = {
            "timestamp": timestamp,
            "total_procesados": len(procesados),
            "total_relevantes": len(relevantes),
            "total_potenciales": len(potenciales),
            "procesados": procesados,
            "relevantes": relevantes,
            "potenciales": potenciales
        }
        
        try:
            with open(f"scraping_results_{timestamp}.json", "w", encoding="utf-8") as f:
                json.dump(resultados, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Resultados guardados en: scraping_results_{timestamp}.json")
        except Exception as e:
            logger.warning(f"⚠️ Error al guardar resultados: {e}")

        # Resumen final mejorado
        logger.info("📦 RESUMEN FINAL DEL SCRAPING ANTIDETECCIÓN")
        logger.info(f"📊 Guardados totales: {len(procesados)}")
        logger.info(f"🟢 Relevantes (telegram): {len(relevantes)}")
        logger.info(f"🟡 Potenciales: {len(potenciales)}")
        
        if relevantes:
            logger.info("\n🎯 TOP 5 RELEVANTES:")
            for i, r in enumerate(relevantes[:5], 1):
                clean_msg = r.replace("*", "").replace("🚘", f"{i}.")
                logger.info(clean_msg)
        
        if potenciales:
            logger.info(f"\n💡 Potenciales disponibles: {len(potenciales)}")

        logger.info("✅ Scraping completado exitosamente!")

    asyncio.run(main())
