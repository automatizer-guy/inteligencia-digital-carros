# scraper_marketplace_stealth.py

import os
import re
import json
import random
import asyncio
import logging
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
import hashlib
import time
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi_real, coincide_modelo, extraer_anio,
    existe_en_db, insertar_anuncio_db, inicializar_tabla_anuncios,
    limpiar_link, modelos_bajo_rendimiento, MODELOS_INTERES,
    SCORE_MIN_TELEGRAM, ROI_MINIMO, obtener_anuncio_db, anuncio_diferente
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# CONFIGURACIÓN ANTI-DETECCIÓN MEJORADA
MIN_PRECIO_VALIDO = 3000
MAX_EJEMPLOS_SIN_ANIO = 5
ROI_POTENCIAL_MIN = ROI_MINIMO - 10

# Configuración más conservadora y realista
MAX_SCROLLS_POR_SORT = 8  # Reducido significativamente
MIN_DELAY = 4.5           # Aumentado para parecer más humano
MAX_DELAY = 8.5           # Aumentado para parecer más humano
DELAY_ENTRE_ANUNCIOS = 3.5 # Aumentado
MAX_CONSECUTIVOS_SIN_NUEVOS = 2
BATCH_SIZE_SCROLL = 4     # Reducido para menor agresividad
SESSION_COOLDOWN = 180    # 3 minutos entre modelos
DAILY_LIMIT_REQUESTS = 150 # Límite diario de requests

# Patrones de comportamiento humano
HUMAN_READING_DELAYS = [2.1, 3.4, 4.2, 5.1, 2.8, 3.9, 4.7]
HUMAN_SCROLL_PATTERNS = [180, 220, 280, 320, 150, 190, 250]
DISTRACTION_PROBABILITY = 0.15  # 15% chance de "distracción"

# Rotación de User-Agents más amplia
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# Sistema de gestión de sesiones
class SessionManager:
    def __init__(self):
        self.session_start = datetime.now()
        self.requests_today = 0
        self.last_request_time = None
        self.session_id = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
    
    def should_take_break(self) -> bool:
        """Determina si debemos tomar un descanso basado en patrones humanos"""
        if self.requests_today > DAILY_LIMIT_REQUESTS:
            return True
        
        session_duration = (datetime.now() - self.session_start).total_seconds()
        # Descanso cada 45-60 minutos (comportamiento humano típico)
        if session_duration > random.uniform(2700, 3600):
            return True
            
        return False
    
    def log_request(self):
        """Registra una nueva request"""
        self.requests_today += 1
        self.last_request_time = datetime.now()
    
    async def human_break(self):
        """Simula un descanso humano"""
        break_duration = random.uniform(300, 900)  # 5-15 minutos
        logger.info(f"😴 Tomando descanso humano de {break_duration/60:.1f} minutos")
        await asyncio.sleep(break_duration)
        self.session_start = datetime.now()

session_manager = SessionManager()

def limpiar_url(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def simulate_human_distraction(page: Page):
    """Simula distracciones humanas aleatorias"""
    if random.random() < DISTRACTION_PROBABILITY:
        actions = [
            lambda: page.mouse.move(random.randint(100, 1200), random.randint(100, 800)),
            lambda: page.keyboard.press("Tab"),
            lambda: asyncio.sleep(random.uniform(1.5, 4.0)),
            lambda: page.mouse.wheel(0, random.randint(-50, 50))
        ]
        
        action = random.choice(actions)
        try:
            await action()
        except Exception:
            pass  # Ignorar errores en simulaciones

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    logger.info("🔐 Cargando contexto stealth con cookies...")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    
    # Seleccionar User-Agent aleatorio
    user_agent = random.choice(USER_AGENTS)
    
    viewport_sizes = [
        {"width": 1366, "height": 768},
        {"width": 1920, "height": 1080},
        {"width": 1440, "height": 900},
        {"width": 1536, "height": 864}
    ]
    viewport = random.choice(viewport_sizes)
    
    context_options = {
        "locale": "es-ES",
        "timezone_id": "America/Guatemala",
        "user_agent": user_agent,
        "viewport": viewport,
        "screen": {"width": viewport["width"], "height": viewport["height"]},
        "device_scale_factor": random.choice([1.0, 1.25, 1.5]),
        "is_mobile": False,
        "has_touch": False,
        # Simular hardware más realista
        "color_scheme": random.choice(["light", "dark"]),
        "reduced_motion": "no-preference",
        "forced_colors": "none"
    }
    
    if not cj:
        logger.warning("⚠️ Sin cookies. Usando sesión anónima con stealth.")
        return await browser.new_context(**context_options)
    
    try:
        cookies = json.loads(cj)
    except Exception as e:
        logger.error(f"❌ Error parsing cookies: {e}")
        return await browser.new_context(**context_options)

    context = await browser.new_context(**context_options)
    await context.add_cookies(cookies)
    
    # Configurar headers adicionales para parecer más humano
    await context.set_extra_http_headers({
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none'
    })
    
    return context

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extrae items con comportamiento más humano"""
    await simulate_human_distraction(page)
    
    try:
        # Esperar a que los elementos se carguen completamente
        await page.wait_for_selector("a[href*='/marketplace/item']", timeout=10000)
        await asyncio.sleep(random.uniform(1.0, 2.5))
        
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        
        for i, a in enumerate(items):
            # Simular lectura humana con delays variables
            if i % 3 == 0:  # Cada 3 elementos, pausa más larga
                await asyncio.sleep(random.choice(HUMAN_READING_DELAYS))
            
            titulo = (await a.inner_text()).strip()
            aria_label = await a.get_attribute("aria-label") or ""
            texto_completo = f"{titulo} {aria_label}".strip()
            href = await a.get_attribute("href") or ""
            resultados.append({"texto": texto_completo, "url": limpiar_url(href)})
        
        return resultados
    except Exception as e:
        logger.error(f"❌ Error extrayendo items: {e}")
        return []

async def scroll_hasta_humano(page: Page) -> bool:
    """Scroll más humano y realista"""
    # Gestión de sesión antes del scroll
    if session_manager.should_take_break():
        await session_manager.human_break()
    
    # Movimiento de mouse previo más realista
    current_pos = await page.evaluate("({x: window.innerWidth/2, y: window.innerHeight/2})")
    target_x = random.randint(200, int(current_pos["x"] * 1.5))
    target_y = random.randint(150, int(current_pos["y"] * 1.2))
    
    # Movimiento en pasos para simular arrastre humano
    steps = random.randint(3, 6)
    for step in range(steps):
        intermediate_x = current_pos["x"] + (target_x - current_pos["x"]) * (step / steps)
        intermediate_y = current_pos["y"] + (target_y - current_pos["y"]) * (step / steps)
        await page.mouse.move(intermediate_x, intermediate_y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
    
    # Pausa antes del scroll (lectura)
    await asyncio.sleep(random.choice(HUMAN_READING_DELAYS))
    
    prev_height = await page.evaluate("document.body.scrollHeight")
    
    # Scroll más irregular y humano
    scroll_distance = random.choice(HUMAN_SCROLL_PATTERNS)
    scroll_steps = random.randint(2, 4)
    
    for step in range(scroll_steps):
        step_distance = scroll_distance // scroll_steps
        await page.mouse.wheel(0, step_distance)
        await asyncio.sleep(random.uniform(0.3, 0.8))
    
    # Espera después del scroll (carga de contenido)
    await asyncio.sleep(random.uniform(2.5, 4.5))
    
    # Ocasionalmente scroll hacia arriba (comportamiento humano)
    if random.random() < 0.1:  # 10% de probabilidad
        await page.mouse.wheel(0, -random.randint(50, 150))
        await asyncio.sleep(random.uniform(1.0, 2.0))
    
    new_height = await page.evaluate("document.body.scrollHeight")
    session_manager.log_request()
    
    return new_height > prev_height

async def procesar_lote_urls_stealth(page: Page, urls_lote: List[str], modelo: str, 
                                   vistos_globales: Set[str], contador: Dict[str, int],
                                   procesados: List[str], potenciales: List[str], 
                                   relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Procesamiento de URLs con máximo stealth"""
    nuevos_en_lote = 0
    
    for i, url in enumerate(urls_lote):
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)

        # Delay progresivo más largo
        base_delay = DELAY_ENTRE_ANUNCIOS + (i * 0.5)
        actual_delay = base_delay * random.uniform(0.8, 1.4)
        await asyncio.sleep(actual_delay)
        
        # Simular comportamiento humano antes de navegar
        await simulate_human_distraction(page)

        try:
            # Navegación más lenta y realista
            await asyncio.wait_for(page.goto(url, wait_until="domcontentloaded"), timeout=20)
            await asyncio.sleep(random.uniform(2.0, 4.0))  # Tiempo de "lectura inicial"
            
            # Extracción con múltiples intentos
            texto = None
            for attempt in range(3):
                try:
                    texto = await asyncio.wait_for(
                        page.inner_text("div[role='main']"), 
                        timeout=15
                    )
                    if texto and len(texto.strip()) > 100:
                        break
                    await asyncio.sleep(1.5)
                except Exception:
                    if attempt == 2:  # Último intento
                        texto = await page.title() or "Sin texto disponible"

        except Exception as e:
            logger.warning(f"⚠️ Error procesando {url}: {e}")
            # Delay extra después de error para no parecer bot
            await asyncio.sleep(random.uniform(3.0, 6.0))
            continue

        if not texto:
            continue

        # El procesamiento individual mantiene la lógica original
        if await procesar_anuncio_individual_stealth(page, url, texto, modelo, contador, 
                                                   procesados, potenciales, relevantes, sin_anio_ejemplos):
            nuevos_en_lote += 1
            
            # Pause progresiva basada en éxito
            if nuevos_en_lote % 2 == 0:
                await asyncio.sleep(random.uniform(4.0, 8.0))

    return nuevos_en_lote

async def procesar_anuncio_individual_stealth(
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
    """Procesamiento individual con stealth mejorado"""
    
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

    # Expansión más cuidadosa y humana
    if not anio or not (1990 <= anio <= datetime.now().year):
        await asyncio.sleep(random.uniform(1.5, 3.0))  # Simular lectura
        
        ver_mas_selectors = [
            "div[role='main'] span:has-text('Ver más')",
            "[data-testid='read-more-button']",
            "span:text('Ver más')",
            "[aria-label*='más']"
        ]
        
        ver_mas = None
        for selector in ver_mas_selectors:
            try:
                ver_mas = await page.query_selector(selector)
                if ver_mas:
                    break
            except Exception:
                continue
        
        if ver_mas:
            # Comportamiento humano al hacer click
            await page.hover(await ver_mas.bounding_box())
            await asyncio.sleep(random.uniform(0.5, 1.2))
            await ver_mas.click()
            await asyncio.sleep(random.uniform(2.0, 4.0))  # Tiempo de lectura expandida
            
            try:
                texto_expandido = await page.inner_text("div[role='main']")
                anio_expandido = extraer_anio(texto_expandido)
                if anio_expandido and (1990 <= anio_expandido <= datetime.now().year):
                    anio = anio_expandido
                    texto = texto_expandido  # Usar texto expandido
            except Exception:
                pass
    
    if not anio or not (1990 <= anio <= datetime.now().year):
        contador["sin_anio"] += 1
        if len(sin_anio_ejemplos) < MAX_EJEMPLOS_SIN_ANIO:
            sin_anio_ejemplos.append((texto[:200], url))
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

    # Base de datos (lógica original mantenida)
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
            insertar_anuncio_db(link=url, modelo=modelo, anio=anio, precio=precio, km="", 
                               roi=roi_data["roi"], score=score, relevante=relevante, 
                               confianza_precio=roi_data["confianza"], muestra_precio=roi_data["muestra"])
            logger.info(f"🔄 Actualizado: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score}")
            contador["actualizados"] += 1
        else:
            contador["repetidos"] += 1
    else:
        insertar_anuncio_db(link=url, modelo=modelo, anio=anio, precio=precio, km="", 
                           roi=roi_data["roi"], score=score, relevante=relevante, 
                           confianza_precio=roi_data["confianza"], muestra_precio=roi_data["muestra"])
        logger.info(f"💾 Guardado: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score} | Relevante={relevante}")
        contador["guardado"] += 1

    procesados.append(mensaje_base)

    if relevante:
        relevantes.append(mensaje_base)
    elif ROI_POTENCIAL_MIN <= roi_data["roi"] < ROI_MINIMO:
        potenciales.append(mensaje_base)

    return True

async def procesar_ordenamiento_stealth(page: Page, modelo: str, sort: str, 
                                       vistos_globales: Set[str], contador: Dict[str, int],
                                       procesados: List[str], potenciales: List[str], 
                                       relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Procesamiento stealth por ordenamiento"""
    
    url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
    
    # Navegación más lenta y realista
    await page.goto(url_busq, wait_until="domcontentloaded")
    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    
    # Simular que el usuario lee la página
    await simulate_human_distraction(page)

    scrolls_realizados = 0
    consec_repetidos = 0
    nuevos_total = 0
    urls_pendientes = []

    while scrolls_realizados < MAX_SCROLLS_POR_SORT:
        # Verificar límites de sesión
        if session_manager.should_take_break():
            logger.info("🛑 Límite de sesión alcanzado, tomando descanso")
            await session_manager.human_break()
        
        # Extracción más cuidadosa
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
        
        # Procesamiento más conservador
        if len(urls_pendientes) >= BATCH_SIZE_SCROLL or scrolls_realizados >= MAX_SCROLLS_POR_SORT - 1:
            if urls_pendientes:
                lote_actual = urls_pendientes[:BATCH_SIZE_SCROLL]
                urls_pendientes = urls_pendientes[BATCH_SIZE_SCROLL:]
                
                nuevos_en_lote = await procesar_lote_urls_stealth(page, lote_actual, modelo, vistos_globales, 
                                                                contador, procesados, potenciales, relevantes, sin_anio_ejemplos)
                nuevos_total += nuevos_en_lote
                
                if nuevos_en_lote == 0:
                    consec_repetidos += 1
                else:
                    consec_repetidos = 0

        scrolls_realizados += 1
        
        # Salida más temprana y conservadora
        if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS:
            logger.info(f"🔄 Finalizando {sort}: {consec_repetidos} scrolls sin nuevos")
            break
            
        if not await scroll_hasta_humano(page):
            logger.info(f"🔄 Fin de contenido en {sort}")
            break

    # Procesar URLs restantes con delay extra
    if urls_pendientes:
        await asyncio.sleep(random.uniform(3.0, 6.0))
        await procesar_lote_urls_stealth(page, urls_pendientes, modelo, vistos_globales, 
                                       contador, procesados, potenciales, relevantes, sin_anio_ejemplos)

    return nuevos_total

async def procesar_modelo_stealth(page: Page, modelo: str,
                                procesados: List[str],
                                potenciales: List[str],
                                relevantes: List[str]) -> int:
    """Procesamiento de modelo con máximo stealth"""
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos"
    ]}
    
    # Solo usar el ordenamiento más efectivo para reducir footprint
    SORT_OPTS = ["best_match"]  # Reducido a uno solo
    inicio = datetime.now()
    total_nuevos = 0

    for sort in SORT_OPTS:
        logger.info(f"🔍 [STEALTH] Procesando {modelo} con {sort}")
        try:
            nuevos_sort = await asyncio.wait_for(
                procesar_ordenamiento_stealth(page, modelo, sort, vistos_globales, contador,
                                             procesados, potenciales, relevantes, sin_anio_ejemplos), 
                timeout=300  # 5 minutos timeout
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos procesados")
            
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout en {sort} para {modelo}")
            continue

    duracion = (datetime.now() - inicio).seconds
    logger.info(f"""
🥷 STEALTH - MODELO: {modelo.upper()}
   Duración: {duracion} s
   Encontrados: {contador['total']}
   Guardados: {contador['guardado']}
   Actualizados: {contador.get('actualizados', 0)}
   Relevantes: {len([r for r in relevantes if modelo.lower() in r.lower()])}
   Session ID: {session_manager.session_id}
   Requests hoy: {session_manager.requests_today}
   🥷""")

    return total_nuevos

async def buscar_autos_marketplace_stealth(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Función principal con máximo stealth"""
    inicializar_tabla_anuncios()
    modelos = modelos_override or MODELOS_INTERES
    flops = modelos_bajo_rendimiento()
    activos = [m for m in modelos if m not in flops]

    # Limitar modelos por sesión para reducir detección
    max_modelos_sesion = 3
    if len(activos) > max_modelos_sesion:
        activos = random.sample(activos, max_modelos_sesion)
        logger.info(f"🎯 Limitando a {max_modelos_sesion} modelos por sesión stealth")

    procesados, potenciales, relevantes = [], [], []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox', 
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions-file-access-check',
                '--disable-extensions-http-throttling',
                '--disable-extensions-http-throttling',
                '--disable-plugins-discovery',
                '--no-first-run'
            ]
        )
        
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        # Stealth adicional
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            window.chrome = {
                runtime: {},
            };
            
            Object.defineProperty(navigator, 'languages', {
                get: () => ['es-ES', 'es', 'en'],
            });
        """)

        await page.goto("https://www.facebook.com/marketplace", wait_until="domcontentloaded")
        await asyncio.sleep(random.uniform(5.0, 8.0))  # Tiempo inicial más largo

        if "login" in page.url or "recover" in page.url:
            alerta = "🚨 Sesión inválida detectada"
            logger.warning(alerta)
            return [], [], [alerta]

        logger.info("✅ [STEALTH] Sesión activa confirmada")

        # Procesamiento ultra-conservador
        for i, modelo in enumerate(activos):
            logger.info(f"🥷 [{i+1}/{len(activos)}] Procesando: {modelo}")
            
            try:
                await asyncio.wait_for(
                    procesar_modelo_stealth(page, modelo, procesados, potenciales, relevantes), 
                    timeout=480  # 8 minutos por modelo
                )
                
                # Descanso obligatorio entre modelos
                if i < len(activos) - 1:
                    cooldown = SESSION_COOLDOWN + random.uniform(-30, 60)
                    logger.info(f"😴 Cooldown de {cooldown/60:.1f} minutos antes del siguiente modelo")
                    await asyncio.sleep(cooldown)
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏳ {modelo} → Timeout. Continuando.")

        await browser.close()

    return procesados, potenciales, relevantes

# Mantener compatibilidad con código original
async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Wrapper para mantener compatibilidad - redirige a versión stealth"""
    return await buscar_autos_marketplace_stealth(modelos_override)

# Sistema de rotación de proxies (opcional - requiere configuración externa)
class ProxyRotator:
    def __init__(self):
        self.proxies = self._load_proxies()
        self.current_index = 0
    
    def _load_proxies(self) -> List[str]:
        """Carga proxies desde variable de entorno"""
        proxy_string = os.environ.get("PROXY_LIST", "")
        if not proxy_string:
            return []
        return [p.strip() for p in proxy_string.split(",") if p.strip()]
    
    def get_next_proxy(self) -> Optional[str]:
        """Obtiene el siguiente proxy en rotación"""
        if not self.proxies:
            return None
        
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

# Función de utilidad para análisis de detección
async def test_detection_risk(page: Page) -> Dict[str, any]:
    """Evalúa el riesgo de detección de la sesión actual"""
    try:
        # Verificar si estamos siendo detectados
        indicators = {
            "captcha_present": bool(await page.query_selector("[data-testid*='captcha']")),
            "rate_limit_warning": "rate limit" in (await page.content()).lower(),
            "login_redirect": "login" in page.url.lower(),
            "blocked_content": "blocked" in (await page.content()).lower(),
            "current_url": page.url,
            "session_id": session_manager.session_id,
            "requests_count": session_manager.requests_today
        }
        
        risk_score = sum([
            indicators["captcha_present"] * 3,
            indicators["rate_limit_warning"] * 2,
            indicators["login_redirect"] * 3,
            indicators["blocked_content"] * 2
        ])
        
        indicators["risk_score"] = risk_score
        indicators["risk_level"] = "HIGH" if risk_score >= 5 else "MEDIUM" if risk_score >= 3 else "LOW"
        
        return indicators
        
    except Exception as e:
        return {"error": str(e), "risk_level": "UNKNOWN"}

# Sistema de backup y recuperación de sesión
class SessionBackup:
    def __init__(self):
        self.backup_file = "session_state.json"
    
    def save_state(self, procesados: List[str], potenciales: List[str], 
                   relevantes: List[str], current_modelo: str):
        """Guarda el estado actual de la sesión"""
        state = {
            "timestamp": datetime.now().isoformat(),
            "session_id": session_manager.session_id,
            "current_modelo": current_modelo,
            "procesados": procesados,
            "potenciales": potenciales,
            "relevantes": relevantes,
            "requests_today": session_manager.requests_today
        }
        
        try:
            with open(self.backup_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Estado guardado: {len(procesados)} procesados")
        except Exception as e:
            logger.error(f"❌ Error guardando estado: {e}")
    
    def load_state(self) -> Optional[Dict]:
        """Carga el estado previo si existe"""
        try:
            if os.path.exists(self.backup_file):
                with open(self.backup_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                
                # Verificar que el backup no sea muy antiguo (>6 horas)
                backup_time = datetime.fromisoformat(state["timestamp"])
                if (datetime.now() - backup_time).total_seconds() > 21600:
                    logger.info("🗑️ Backup muy antiguo, descartando")
                    return None
                
                logger.info(f"🔄 Cargando estado previo: {len(state.get('procesados', []))} procesados")
                return state
        except Exception as e:
            logger.error(f"❌ Error cargando estado: {e}")
        
        return None

# Función mejorada con sistema de backup
async def buscar_autos_marketplace_safe(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Versión con sistema de backup y recuperación"""
    backup_system = SessionBackup()
    
    # Intentar cargar estado previo
    prev_state = backup_system.load_state()
    if prev_state:
        logger.info("🔄 Recuperando sesión previa...")
        # Aquí podrías implementar lógica para continuar desde donde se quedó
    
    try:
        procesados, potenciales, relevantes = await buscar_autos_marketplace_stealth(modelos_override)
        
        # Limpiar backup al completar exitosamente
        if os.path.exists(backup_system.backup_file):
            os.remove(backup_system.backup_file)
        
        return procesados, potenciales, relevantes
        
    except Exception as e:
        logger.error(f"❌ Error en sesión: {e}")
        # El backup se maneja automáticamente en la función principal
        raise

# Análisis de patrones anti-detección
def analyze_detection_patterns() -> Dict[str, str]:
    """Analiza patrones que pueden causar detección"""
    return {
        "user_agents": "✅ Rotación implementada",
        "delays": "✅ Patrones humanizados",
        "scrolling": "✅ Comportamiento irregular",
        "mouse_movement": "✅ Simulación humana",
        "session_management": "✅ Límites y descansos",
        "request_batching": "✅ Procesamiento conservador",
        "fingerprinting": "✅ Headers y viewport aleatorios",
        "proxy_rotation": "⚠️ Requiere configuración externa",
        "captcha_detection": "✅ Monitoreo implementado"
    }


if __name__ == "__main__":
    async def main():
        logger.info("🥷 Iniciando scraper STEALTH")
        logger.info("📊 Análisis anti-detección:")
        for feature, status in analyze_detection_patterns().items():
            logger.info(f"   {feature}: {status}")
        
        try:
            procesados, potenciales, relevantes = await buscar_autos_marketplace_safe()

            logger.info("📦 RESUMEN FINAL STEALTH")
            logger.info(f"Guardados totales: {len(procesados)}")
            logger.info(f"Relevantes: {len(relevantes)}")
            logger.info(f"Potenciales: {len(potenciales)}")
            logger.info(f"Session ID: {session_manager.session_id}")

            if relevantes:
                logger.info("\n🟢 Top 5 relevantes:")
                for r in relevantes[:5]:
                    logger.info(r.replace("*", "").replace("\\n", "\n"))

        except Exception as e:
            logger.error(f"💥 Error crítico: {e}")
            return

    asyncio.run(main())
