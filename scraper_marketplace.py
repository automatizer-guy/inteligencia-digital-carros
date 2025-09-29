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
from playwright.async_api import async_playwright, Browser, Page, BrowserContext, Error as PlaywrightError, Playwright
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

# Configuración optimizada
MAX_SCROLLS_POR_SORT = 12  # Reducido más para evitar detección
MIN_DELAY = 2
MAX_DELAY = 4
DELAY_ENTRE_ANUNCIOS = 2
MAX_CONSECUTIVOS_SIN_NUEVOS = 3
BATCH_SIZE_SCROLL = 6  # Reducido para procesar más rápido

class BrowserManager:
    """Gestiona el ciclo de vida del navegador y contextos"""
    def __init__(self, playwright: Playwright):
        self.playwright = playwright
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
    async def inicializar(self):
        """Inicializa el navegador y contexto"""
        logger.info("🚀 Inicializando navegador...")
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-hang-monitor',
                '--disable-prompt-on-repost',
                '--disable-sync',
                '--force-color-profile=srgb',
                '--metrics-recording-only',
                '--no-first-run',
                '--mute-audio',
                '--hide-scrollbars',
                '--disable-infobars',
                '--window-size=1920,1080',
                '--disable-features=TranslateUI,BlinkGenPropertyTrees'
            ]
        )
        await self.criar_contexto()
        
    async def crear_contexto(self):
        """Crea un nuevo contexto con cookies"""
        logger.info("🔐 Creando contexto con cookies...")
        cj = os.environ.get("FB_COOKIES_JSON", "")
        
        if not cj:
            logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
            self.context = await self.browser.new_context(locale="es-ES")
        else:
            try:
                cookies_raw = json.loads(cj)
                
                # Limpiar y validar cookies
                cookies_limpias = []
                for cookie in cookies_raw:
                    cookie_limpia = {
                        "name": cookie.get("name"),
                        "value": cookie.get("value"),
                        "domain": cookie.get("domain"),
                        "path": cookie.get("path", "/")
                    }
                    
                    if "expires" in cookie and isinstance(cookie["expires"], (int, float)) and cookie["expires"] > 0:
                        cookie_limpia["expires"] = cookie["expires"]
                    
                    if "httpOnly" in cookie:
                        cookie_limpia["httpOnly"] = cookie["httpOnly"]
                    
                    if "secure" in cookie:
                        cookie_limpia["secure"] = cookie["secure"]
                    
                    if "sameSite" in cookie and cookie["sameSite"] in ["Strict", "Lax", "None"]:
                        cookie_limpia["sameSite"] = cookie["sameSite"]
                    
                    cookies_limpias.append(cookie_limpia)
                
                self.context = await self.browser.new_context(
                    locale="es-ES",
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
                    viewport={"width": 1920, "height": 1080}
                )
                await self.context.add_cookies(cookies_limpias)
            except Exception as e:
                logger.error(f"❌ Error al parsear cookies: {e}")
                self.context = await self.browser.new_context(locale="es-ES")
        
        await self.crear_pagina()
        
    async def crear_pagina(self):
        """Crea una nueva página"""
        if self.context:
            self.page = await self.context.new_page()
            await self.page.set_extra_http_headers({
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
            })
            logger.info("✅ Nueva página creada")
    
    async def verificar_y_recrear(self) -> bool:
        """Verifica el estado y recrea si es necesario"""
        try:
            # Verificar navegador
            if not self.browser or not self.browser.is_connected():
                logger.error("❌ Navegador desconectado - no se puede recuperar")
                return False
            
            # Verificar contexto
            if not self.context:
                logger.warning("🔄 Recreando contexto...")
                await self.crear_contexto()
                return True
            
            # Verificar página
            if not self.page or self.page.is_closed():
                logger.warning("🔄 Recreando página...")
                await self.crear_pagina()
                return True
            
            # Verificar que la página realmente funciona
            try:
                await asyncio.wait_for(self.page.evaluate("1"), timeout=2)
                return True
            except Exception:
                logger.warning("🔄 Página no responde, recreando...")
                await self.crear_pagina()
                return True
                
        except Exception as e:
            logger.error(f"❌ Error verificando estado: {e}")
            return False
    
    async def cerrar(self):
        """Cierra todos los recursos"""
        try:
            if self.page and not self.page.is_closed():
                await self.page.close()
                logger.info("✅ Página cerrada")
        except Exception as e:
            logger.warning(f"Error cerrando página: {e}")
        
        try:
            if self.context:
                await self.context.close()
                logger.info("✅ Contexto cerrado")
        except Exception as e:
            logger.warning(f"Error cerrando contexto: {e}")
        
        try:
            if self.browser:
                await self.browser.close()
                logger.info("✅ Navegador cerrado")
        except Exception as e:
            logger.warning(f"Error cerrando navegador: {e}")

def limpiar_url(link: str) -> str:
    """Limpia y normaliza URLs de Facebook Marketplace"""
    if not link:
        return ""
    try:
        path = urlparse(link.strip()).path.rstrip("/")
        return f"https://www.facebook.com{path}"
    except Exception as e:
        logger.warning(f"Error limpiando URL {link}: {e}")
        return ""

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extrae items de anuncios de la página actual"""
    try:
        items = await asyncio.wait_for(
            page.query_selector_all("a[href*='/marketplace/item']"),
            timeout=10
        )
        resultados = []
        for a in items:
            try:
                titulo = (await a.inner_text()).strip()
                aria_label = await a.get_attribute("aria-label") or ""
                texto_completo = f"{titulo} {aria_label}".strip()
                href = await a.get_attribute("href") or ""
                resultados.append({"texto": texto_completo, "url": limpiar_url(href)})
            except Exception:
                continue
        return resultados
    except Exception as e:
        logger.warning(f"Error extrayendo items: {e}")
        return []

async def scroll_hasta(page: Page) -> bool:
    """Realiza scroll simulando comportamiento humano"""
    try:
        # Movimiento de mouse más rápido
        await asyncio.wait_for(
            page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600)
            ),
            timeout=3
        )
        await asyncio.sleep(random.uniform(0.2, 0.5))

        prev = await asyncio.wait_for(
            page.evaluate("document.body.scrollHeight"),
            timeout=3
        )

        await asyncio.wait_for(
            page.mouse.wheel(0, random.randint(150, 300)),
            timeout=3
        )
        await asyncio.sleep(random.uniform(0.8, 1.5))

        now = await asyncio.wait_for(
            page.evaluate("document.body.scrollHeight"),
            timeout=3
        )

        return now > prev
    except Exception as e:
        logger.warning(f"Error en scroll: {e}")
        return False

async def extraer_texto_anuncio(page: Page, url: str) -> str:
    """Extrae texto del anuncio con múltiples estrategias de fallback"""
    texto = "Sin texto disponible"
    
    try:
        # Estrategia 1: Contenido principal
        try:
            texto_extraido = await asyncio.wait_for(
                page.inner_text("div[role='main']"), 
                timeout=8
            )
            if texto_extraido and len(texto_extraido.strip()) >= 100:
                return texto_extraido.strip()
        except Exception:
            pass

        # Estrategia 2: Título de la página
        try:
            texto_title = await asyncio.wait_for(page.title(), timeout=5)
            if texto_title and len(texto_title.strip()) > 10:
                texto = texto_title.strip()
        except Exception:
            pass

        # Estrategia 3: Meta description
        try:
            meta_desc = await asyncio.wait_for(
                page.get_attribute('meta[name="description"]', 'content'),
                timeout=5
            )
            if meta_desc and len(meta_desc.strip()) > len(texto):
                texto = meta_desc.strip()
        except Exception:
            pass

        # Estrategia 4: Contenido del body
        try:
            if texto == "Sin texto disponible":
                body_text = await asyncio.wait_for(
                    page.inner_text("body"),
                    timeout=5
                )
                if body_text and len(body_text.strip()) > 50:
                    texto = body_text.strip()[:500]
        except Exception:
            pass

    except Exception as e:
        logger.warning(f"Error extrayendo texto de {url}: {e}")
    
    return texto

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
    
    try:
        texto = texto.strip()
        if not texto or len(texto) < 10:
            contador["texto_vacio"] = contador.get("texto_vacio", 0) + 1
            return False
            
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

        if not anio or not (1990 <= anio <= datetime.now().year):
            try:
                ver_mas = await asyncio.wait_for(
                    page.query_selector("div[role='main'] span:has-text('Ver más')"),
                    timeout=3
                )
                if ver_mas:
                    await ver_mas.click()
                    await asyncio.sleep(1.0)
                    texto_expandido = await asyncio.wait_for(
                        page.inner_text("div[role='main']"),
                        timeout=5
                    )
                    anio_expandido = extraer_anio(texto_expandido)
                    if anio_expandido and (1990 <= anio_expandido <= datetime.now().year):
                        anio = anio_expandido
                        texto = texto_expandido
            except Exception:
                pass
        
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

        try:
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
                logger.info(f"💾 Guardado nuevo: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score}")
                contador["guardado"] += 1
        except Exception as e:
            logger.error(f"Error en DB para {url}: {e}")
            contador["error_db"] = contador.get("error_db", 0) + 1
            return False

        procesados.append(mensaje_base)

        if relevante:
            relevantes.append(mensaje_base)
        elif ROI_POTENCIAL_MIN <= roi_data["roi"] < ROI_MINIMO:
            potenciales.append(mensaje_base)

        return True

    except Exception as e:
        logger.error(f"Error en procesar_anuncio_individual: {e}")
        contador["error_general"] = contador.get("error_general", 0) + 1
        return False

async def procesar_lote_urls(
    browser_manager: BrowserManager,
    urls_lote: List[str],
    modelo: str,
    vistos_globales: Set[str],
    contador: Dict[str, int],
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str],
    sin_anio_ejemplos: List[Tuple[str, str]]
) -> int:
    """Procesa un lote de URLs con manejo robusto de errores"""
    nuevos_en_lote = 0

    for url in urls_lote:
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)

        # Verificar y recrear si es necesario
        if not await browser_manager.verificar_y_recrear():
            logger.error("❌ No se pudo recuperar el navegador")
            return nuevos_en_lote
        
        try:
            await asyncio.wait_for(
                browser_manager.page.goto(url, wait_until='domcontentloaded'),
                timeout=12
            )
            await asyncio.sleep(random.uniform(2.0, 3.5))
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout navegando a {url}")
            contador["timeout"] = contador.get("timeout", 0) + 1
            continue
        except Exception as e:
            logger.warning(f"Error navegando a {url}: {e}")
            contador["error"] += 1
            continue

        try:
            await asyncio.sleep(DELAY_ENTRE_ANUNCIOS)
            texto = await extraer_texto_anuncio(browser_manager.page, url)
            
            if len(texto.strip()) < 10:
                contador["texto_insuficiente"] = contador.get("texto_insuficiente", 0) + 1
                continue
            
            if await procesar_anuncio_individual(
                browser_manager.page, url, texto, modelo, contador,
                procesados, potenciales, relevantes, sin_anio_ejemplos
            ):
                nuevos_en_lote += 1
                
                if nuevos_en_lote % 3 == 0:
                    await asyncio.sleep(random.uniform(1.5, 2.5))
        except Exception as e:
            logger.error(f"Error procesando {url}: {e}")
            contador["error_procesamiento"] = contador.get("error_procesamiento", 0) + 1
            continue
    
    return nuevos_en_lote

async def procesar_ordenamiento_optimizado(
    browser_manager: BrowserManager,
    modelo: str,
    sort: str,
    vistos_globales: Set[str],
    contador: Dict[str, int],
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str],
    sin_anio_ejemplos: List[Tuple[str, str]]
) -> int:
    """Versión optimizada del procesamiento por ordenamiento"""
    
    if not await browser_manager.verificar_y_recrear():
        logger.error("❌ No se pudo verificar navegador")
        return 0
    
    try:
        url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
        await asyncio.wait_for(
            browser_manager.page.goto(url_busq, wait_until='domcontentloaded'),
            timeout=25
        )
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        scrolls_realizados = 0
        consec_repetidos = 0
        nuevos_total = 0
        urls_pendientes = []

        while scrolls_realizados < MAX_SCROLLS_POR_SORT:
            # Verificar antes de cada scroll
            if not await browser_manager.verificar_y_recrear():
                logger.error(f"❌ Navegador no disponible en scroll {scrolls_realizados}")
                break
            
            try:
                items = await extraer_items_pagina(browser_manager.page)
                urls_nuevas = []
                
                for itm in items:
                    url = limpiar_link(itm["url"])
                    contador["total"] += 1

                    if url and url.startswith("https://www.facebook.com/marketplace/item/") and url not in vistos_globales:
                        urls_nuevas.append(url)

                urls_pendientes.extend(urls_nuevas)
                
                if len(urls_pendientes) >= BATCH_SIZE_SCROLL or scrolls_realizados >= MAX_SCROLLS_POR_SORT - 1:
                    if urls_pendientes:
                        lote_actual = urls_pendientes[:BATCH_SIZE_SCROLL]
                        urls_pendientes = urls_pendientes[BATCH_SIZE_SCROLL:]
                        
                        nuevos_en_lote = await procesar_lote_urls(
                            browser_manager, lote_actual, modelo, vistos_globales,
                            contador, procesados, potenciales, relevantes, sin_anio_ejemplos
                        )
                        
                        nuevos_total += nuevos_en_lote
                        
                        if nuevos_en_lote == 0:
                            consec_repetidos += 1
                        else:
                            consec_repetidos = 0

            except Exception as e:
                logger.warning(f"Error en scroll {scrolls_realizados}: {e}")

            scrolls_realizados += 1
            
            if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS and len(urls_nuevas) < 2:
                logger.info(f"🔄 Salida temprana en {sort}")
                break
                
            if not await scroll_hasta(browser_manager.page):
                logger.info(f"🔄 Fin de contenido en {sort}")
                break

        if urls_pendientes:
            await procesar_lote_urls(
                browser_manager, urls_pendientes, modelo, vistos_globales,
                contador, procesados, potenciales, relevantes, sin_anio_ejemplos
            )

        return nuevos_total
        
    except Exception as e:
        logger.error(f"❌ Error en ordenamiento {sort}: {e}")
        return 0

async def procesar_modelo(
    browser_manager: BrowserManager,
    modelo: str,
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str]
) -> int:
    """Procesa un modelo específico con todos los ordenamientos"""
    
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos", "error", "timeout", "texto_insuficiente",
        "error_procesamiento", "error_db", "error_general", "texto_vacio"
    ]}
    
    SORT_OPTS = ["best_match", "price_asc"]
    inicio = datetime.now()
    total_nuevos = 0

    for sort in SORT_OPTS:
        if not await browser_manager.verificar_y_recrear():
            logger.error(f"❌ No se pudo recuperar navegador para {sort}")
            break
        
        logger.info(f"🔍 Procesando {modelo} con ordenamiento: {sort}")
        try:
            nuevos_sort = await asyncio.wait_for(
                procesar_ordenamiento_optimizado(
                    browser_manager, modelo, sort, vistos_globales, contador,
                    procesados, potenciales, relevantes, sin_anio_ejemplos
                ),
                timeout=150
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos anuncios")
            
            if sort != SORT_OPTS[-1]:
                await asyncio.sleep(random.uniform(3.0, 5.0))
            
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout en {sort} para {modelo}")
            continue
        except Exception as e:
            logger.error(f"❌ Error en {sort} para {modelo}: {e}")
            continue

    duracion = (datetime.now() - inicio).seconds
    
    logger.info(f"""
✨ MODELO: {modelo.upper()}
   Duración: {duracion}s | Guardados: {contador['guardado']} | Relevantes: {len([r for r in relevantes if modelo.lower() in r.lower()])}
   Filtrados: Duplicados={contador['duplicado']}, Sin año={contador['sin_anio']}, Precio bajo={contador['precio_bajo']}
   ✨""")

    return total_nuevos

async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Función principal de búsqueda en Marketplace"""
    
    try:
        inicializar_tabla_anuncios()
        modelos = modelos_override or MODELOS_INTERES
        flops = modelos_bajo_rendimiento()
        activos = [m for m in modelos if m not in flops]
        
        if not activos:
            logger.warning("⚠️ No hay modelos activos por rendimiento. Usando todos.")
            activos = modelos

        procesados, potenciales, relevantes = [], [], []

        async with async_playwright() as p:
            browser_manager = BrowserManager(p)
            
            try:
                await browser_manager.inicializar()
                
                await asyncio.wait_for(
                    browser_manager.page.goto("https://www.facebook.com/marketplace", wait_until='domcontentloaded'),
                    timeout=30
                )
                await asyncio.sleep(3)

                if "login" in browser_manager.page.url or "recover" in browser_manager.page.url:
                    alerta = "🚨 Sesión inválida. Verifica FB_COOKIES_JSON."
                    logger.warning(alerta)
                    return [], [], [alerta]

                logger.info("✅ Sesión activa en Marketplace.")

                modelos_shuffled = activos.copy()
                random.shuffle(modelos_shuffled)

                for i, m in enumerate(modelos_shuffled):
                    if not await browser_manager.verificar_y_recrear():
                        logger.error(f"❌ No se pudo recuperar navegador para {m}")
                        break
                    
                    logger.info(f"📋 Modelo {i+1}/{len(modelos_shuffled)}: {m}")
                    try:
                        await asyncio.wait_for(
                            procesar_modelo(browser_manager, m, procesados, potenciales, relevantes),
                            timeout=300
                        )
                        
                        if i < len(modelos_shuffled) - 1:
                            await asyncio.sleep(random.uniform(8.0, 12.0))
                            
                    except asyncio.TimeoutError:
                        logger.warning(f"⏳ {m} → Timeout")
                    except Exception as e:
                        logger.error(f"❌ Error en {m}: {e}")

            finally:
                await browser_manager.cerrar()

        return procesados, potenciales, relevantes
        
    except Exception as e:
        logger.error(f"❌ Error general: {e}")
        return [], [], [f"🚨 Error: {str(e)}"]

if __name__ == "__main__":
    async def main():
        try:
            procesados, potenciales, relevantes = await buscar_autos_marketplace()

            logger.info("📦 Resumen final")
            logger.info(f"Guardados: {len(procesados)} | Relevantes: {len(relevantes)} | Potenciales: {len(potenciales)}")

            if relevantes:
                logger.info("\n🟢 Relevantes:")
                for r in relevantes[:10]:
                    logger.info(r.replace("*", "").replace("\\n", "\n"))
                
        except Exception as e:
            logger.error(f"❌ Error en main: {e}")

    asyncio.run(main())
