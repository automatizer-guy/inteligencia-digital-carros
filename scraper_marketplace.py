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

# Configuración conservadora
MAX_SCROLLS_POR_SORT = 12  # Reducido ligeramente para evitar crashes
MIN_DELAY = 2.5
MAX_DELAY = 4.5
DELAY_ENTRE_ANUNCIOS = 2.5
MAX_CONSECUTIVOS_SIN_NUEVOS = 3
BATCH_SIZE_SCROLL = 6  # Reducido para procesar en lotes más pequeños
MAX_REINTENTOS_CRASH = 2  # Máximo reintentos por crash

# User-agents más conservadores
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

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

async def verificar_pagina_activa(page: Page) -> bool:
    """Verifica si la página sigue activa y funcional"""
    try:
        await asyncio.wait_for(page.evaluate("document.readyState"), timeout=5)
        return True
    except Exception:
        return False

async def recrear_pagina_si_necesario(context: BrowserContext, page: Page) -> Page:
    """Recrea la página si ha crasheado"""
    try:
        if not await verificar_pagina_activa(page):
            logger.warning("🔄 Página crasheada, recreando...")
            try:
                await page.close()
            except:
                pass
            
            nueva_pagina = await context.new_page()
            await nueva_pagina.set_extra_http_headers({
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
            })
            
            # Navegar de nuevo a Marketplace
            await nueva_pagina.goto("https://www.facebook.com/marketplace", wait_until='domcontentloaded')
            await asyncio.sleep(random.uniform(3, 5))
            
            logger.info("✅ Página recreada exitosamente")
            return nueva_pagina
        return page
    except Exception as e:
        logger.error(f"❌ Error recreando página: {e}")
        return page

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    """Carga el contexto del browser con cookies de Facebook"""
    logger.info("🔐 Cargando cookies desde entorno…")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
        return await browser.new_context(locale="es-ES")
    
    try:
        cookies = json.loads(cj)
        context = await browser.new_context(
            locale="es-ES",
            user_agent=random.choice(USER_AGENTS)
        )
        await context.add_cookies(cookies)
        return context
    except Exception as e:
        logger.error(f"❌ Error al parsear FB_COOKIES_JSON: {e}")
        return await browser.new_context(locale="es-ES")

async def extraer_items_pagina_seguro(page: Page) -> List[Dict[str, str]]:
    """Extrae items con manejo de crashes"""
    try:
        if not await verificar_pagina_activa(page):
            logger.warning("⚠️ Página inactiva durante extracción")
            return []
            
        # Usar timeout más corto para evitar hangs
        items = await asyncio.wait_for(
            page.query_selector_all("a[href*='/marketplace/item']"), 
            timeout=10
        )
        
        resultados = []
        for a in items:
            try:
                titulo = await asyncio.wait_for(a.inner_text(), timeout=3)
                titulo = titulo.strip()
                aria_label = await a.get_attribute("aria-label") or ""
                texto_completo = f"{titulo} {aria_label}".strip()
                href = await a.get_attribute("href") or ""
                resultados.append({"texto": texto_completo, "url": limpiar_url(href)})
            except Exception as e:
                logger.warning(f"Error extrayendo item individual: {e}")
                continue
        return resultados
    except Exception as e:
        logger.warning(f"Error al extraer items de página: {e}")
        return []

async def scroll_seguro(page: Page) -> bool:
    """Realiza scroll con manejo de crashes"""
    try:
        if not await verificar_pagina_activa(page):
            return False
            
        # Movimiento de mouse más conservador
        await page.mouse.move(
            random.randint(200, 700),
            random.randint(200, 500),
            steps=3
        )
        await asyncio.sleep(random.uniform(0.5, 1.0))

        # Evaluar altura inicial
        prev = await asyncio.wait_for(
            page.evaluate("document.body.scrollHeight"), 
            timeout=5
        )

        # Scroll más conservador
        await page.mouse.wheel(0, random.randint(200, 400))
        await asyncio.sleep(random.uniform(2.0, 3.0))

        # Evaluar nueva altura
        now = await asyncio.wait_for(
            page.evaluate("document.body.scrollHeight"), 
            timeout=5
        )

        return now > prev
    except Exception as e:
        logger.warning(f"Error durante scroll: {e}")
        return False

async def navegar_seguro(page: Page, url: str, max_reintentos: int = 2) -> bool:
    """Navega con reintentos en caso de crash"""
    for intento in range(max_reintentos + 1):
        try:
            if not await verificar_pagina_activa(page):
                logger.warning(f"Página inactiva antes de navegar (intento {intento + 1})")
                return False
                
            await asyncio.wait_for(
                page.goto(url, wait_until='domcontentloaded'), 
                timeout=15
            )
            await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            
            # Verificar que la navegación fue exitosa
            if await verificar_pagina_activa(page):
                return True
                
        except Exception as e:
            logger.warning(f"Error navegando (intento {intento + 1}): {e}")
            if intento < max_reintentos:
                await asyncio.sleep(random.uniform(3, 8))
            
    return False

async def extraer_texto_anuncio(page: Page, url: str) -> str:
    """Extrae texto del anuncio con manejo de crashes"""
    texto = "Sin texto disponible"
    
    try:
        if not await verificar_pagina_activa(page):
            return texto
            
        # Estrategia 1: Contenido principal con timeout corto
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
            meta_desc = await page.get_attribute('meta[name="description"]', 'content')
            if meta_desc and len(meta_desc.strip()) > len(texto):
                texto = meta_desc.strip()
        except Exception:
            pass

    except Exception as e:
        logger.warning(f"Error extrayendo texto de {url}: {e}")
    
    return texto

async def procesar_lote_urls(
    context: BrowserContext,
    page: Page,
    urls_lote: List[str],
    modelo: str,
    vistos_globales: Set[str],
    contador: Dict[str, int],
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str],
    sin_anio_ejemplos: List[Tuple[str, str]]
) -> Tuple[int, Page]:
    """Procesa un lote de URLs con recuperación de crashes"""
    nuevos_en_lote = 0

    for url in urls_lote:
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)

        # Verificar y recrear página si es necesario
        page = await recrear_pagina_si_necesario(context, page)
        
        # Navegar de forma segura
        if not await navegar_seguro(page, url):
            contador["error_navegacion"] = contador.get("error_navegacion", 0) + 1
            # Si falla la navegación, recrear página para próximo intento
            page = await recrear_pagina_si_necesario(context, page)
            continue

        # Extraer texto
        texto = await extraer_texto_anuncio(page, url)
        
        if len(texto.strip()) < 10:
            contador["texto_insuficiente"] = contador.get("texto_insuficiente", 0) + 1
            continue

        # Procesar anuncio
        try:
            if await procesar_anuncio_individual(
                page, url, texto, modelo, contador,
                procesados, potenciales, relevantes, sin_anio_ejemplos
            ):
                nuevos_en_lote += 1
                
                # Pausa cada 2 anuncios (más conservador)
                if nuevos_en_lote % 2 == 0:
                    await asyncio.sleep(random.uniform(3.0, 5.0))
        except Exception as e:
            logger.error(f"Error procesando anuncio {url}: {e}")
            contador["error_procesamiento"] = contador.get("error_procesamiento", 0) + 1
    
    return nuevos_en_lote, page

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
    """Procesa un anuncio individual"""
    
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

        # Extraer precio
        m = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
        if not m:
            contador["sin_precio"] += 1
            return False
            
        precio = limpiar_precio(m.group())
        if precio < MIN_PRECIO_VALIDO:
            contador["precio_bajo"] += 1
            return False

        # Extraer año
        anio = extraer_anio(texto)

        # Intento expandir descripción de forma segura
        if not anio or not (1990 <= anio <= datetime.now().year):
            try:
                if await verificar_pagina_activa(page):
                    ver_mas = await page.query_selector("div[role='main'] span:has-text('Ver más')")
                    if ver_mas:
                        await ver_mas.click()
                        await asyncio.sleep(2)
                        texto_expandido = await asyncio.wait_for(
                            page.inner_text("div[role='main']"), 
                            timeout=10
                        )
                        anio_expandido = extraer_anio(texto_expandido)
                        if anio_expandido and (1990 <= anio_expandido <= datetime.now().year):
                            anio = anio_expandido
                            texto = texto_expandido
            except Exception as e:
                logger.warning(f"Error expandiendo descripción: {e}")
        
        if not anio or not (1990 <= anio <= datetime.now().year):
            contador["sin_anio"] += 1
            if len(sin_anio_ejemplos) < MAX_EJEMPLOS_SIN_ANIO:
                sin_anio_ejemplos.append((texto, url))
            return False

        # Calcular ROI y score
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

        # Gestión de base de datos
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
            logger.error(f"Error en base de datos para {url}: {e}")
            contador["error_db"] = contador.get("error_db", 0) + 1
            return False

        # Agregar a listas de resultados
        procesados.append(mensaje_base)

        if relevante:
            relevantes.append(mensaje_base)
        elif ROI_POTENCIAL_MIN <= roi_data["roi"] < ROI_MINIMO:
            potenciales.append(mensaje_base)

        return True

    except Exception as e:
        logger.error(f"Error general procesando anuncio: {e}")
        contador["error_general"] = contador.get("error_general", 0) + 1
        return False

async def procesar_ordenamiento_con_recuperacion(
    context: BrowserContext,
    page: Page,
    modelo: str,
    sort: str,
    vistos_globales: Set[str],
    contador: Dict[str, int],
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str],
    sin_anio_ejemplos: List[Tuple[str, str]]
) -> Tuple[int, Page]:
    """Procesa ordenamiento con recuperación de crashes"""
    
    try:
        # Verificar página antes de empezar
        page = await recrear_pagina_si_necesario(context, page)
        
        url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
        
        if not await navegar_seguro(page, url_busq):
            logger.error(f"❌ No se pudo navegar a la búsqueda para {sort}")
            return 0, page

        scrolls_realizados = 0
        consec_repetidos = 0
        nuevos_total = 0
        urls_pendientes = []

        while scrolls_realizados < MAX_SCROLLS_POR_SORT:
            # Verificar página periódicamente
            if scrolls_realizados % 3 == 0:
                page = await recrear_pagina_si_necesario(context, page)
            
            # Extraer URLs de forma segura
            try:
                items = await extraer_items_pagina_seguro(page)
                urls_nuevas = []
                
                for itm in items:
                    url = limpiar_link(itm["url"])
                    contador["total"] += 1

                    if not url or not url.startswith("https://www.facebook.com/marketplace/item/"):
                        continue
                        
                    if url not in vistos_globales:
                        urls_nuevas.append(url)

                urls_pendientes.extend(urls_nuevas)
                
                # Procesar en lotes más pequeños
                if len(urls_pendientes) >= BATCH_SIZE_SCROLL or scrolls_realizados >= MAX_SCROLLS_POR_SORT - 1:
                    if urls_pendientes:
                        lote_actual = urls_pendientes[:BATCH_SIZE_SCROLL]
                        urls_pendientes = urls_pendientes[BATCH_SIZE_SCROLL:]
                        
                        nuevos_en_lote, page = await procesar_lote_urls(
                            context, page, lote_actual, modelo, vistos_globales, 
                            contador, procesados, potenciales, relevantes, sin_anio_ejemplos
                        )
                        nuevos_total += nuevos_en_lote
                        
                        if nuevos_en_lote == 0:
                            consec_repetidos += 1
                        else:
                            consec_repetidos = 0

            except Exception as e:
                logger.warning(f"Error en scroll {scrolls_realizados}: {e}")
                # Recrear página en caso de error
                page = await recrear_pagina_si_necesario(context, page)

            scrolls_realizados += 1
            
            # Salida temprana
            if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS:
                logger.info(f"🔄 Salida temprana en {sort}")
                break
                
            if not await scroll_seguro(page):
                logger.info(f"🔄 Fin de scroll en {sort}")
                break

        # Procesar URLs restantes
        if urls_pendientes:
            nuevos_final, page = await procesar_lote_urls(
                context, page, urls_pendientes, modelo, vistos_globales, 
                contador, procesados, potenciales, relevantes, sin_anio_ejemplos
            )
            nuevos_total += nuevos_final

        return nuevos_total, page
        
    except Exception as e:
        logger.error(f"Error en procesar_ordenamiento: {e}")
        page = await recrear_pagina_si_necesario(context, page)
        return 0, page

async def procesar_modelo(
    context: BrowserContext,
    page: Page,
    modelo: str,
    procesados: List[str],
    potenciales: List[str],
    relevantes: List[str]
) -> Tuple[int, Page]:
    """Procesa un modelo con manejo de crashes"""
    
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos", "error", "timeout", "texto_insuficiente",
        "error_procesamiento", "error_db", "error_general", "texto_vacio",
        "error_navegacion"
    ]}
    
    SORT_OPTS = ["best_match", "price_asc"]
    inicio = datetime.now()
    total_nuevos = 0

    for sort in SORT_OPTS:
        logger.info(f"🔍 Procesando {modelo} con ordenamiento: {sort}")
        try:
            nuevos_sort, page = await asyncio.wait_for(
                procesar_ordenamiento_con_recuperacion(
                    context, page, modelo, sort, vistos_globales, contador,
                    procesados, potenciales, relevantes, sin_anio_ejemplos
                ), 
                timeout=200  # Timeout ligeramente mayor
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos anuncios procesados")
            
            # Pausa entre ordenamientos
            if sort != SORT_OPTS[-1]:
                await asyncio.sleep(random.uniform(4.0, 8.0))
            
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout en {sort} para {modelo}")
            page = await recrear_pagina_si_necesario(context, page)
        except Exception as e:
            logger.error(f"❌ Error en {sort} para {modelo}: {e}")
            page = await recrear_pagina_si_necesario(context, page)

    duracion = (datetime.now() - inicio).seconds
    
    logger.info(f"""
✨ MODELO: {modelo.upper()}
   Duración: {duracion} s
   Total encontrados: {contador['total']}
   Guardados nuevos: {contador['guardado']}
   Actualizados: {contador.get('actualizados', 0)}
   Repetidos: {contador.get('repetidos', 0)}
   Relevantes: {len([r for r in relevantes if modelo.lower() in r.lower()])}
   Potenciales: {len([p for p in potenciales if modelo.lower() in p.lower()])}
   
   Errores:
   - Errores navegación: {contador.get('error_navegacion', 0)}
   - Errores procesamiento: {contador.get('error_procesamiento', 0)}
   - Error DB: {contador.get('error_db', 0)}
   ✨""")

    return total_nuevos, page

async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Función principal con manejo robusto de crashes"""
    
    try:
        inicializar_tabla_anuncios()
        modelos = modelos_override or MODELOS_INTERES
        flops = modelos_bajo_rendimiento()
        activos = [m for m in modelos if m not in flops]
        
        if not activos:
            logger.warning("⚠️ No hay modelos activos por rendimiento. Usando todos los modelos por defecto.")
            activos = modelos


        procesados, potenciales, relevantes = [], [], []

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled']
            )
            
            try:
                context = await cargar_contexto_con_cookies(browser)
                page = await context.new_page()

                await page.set_extra_http_headers({
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
                })

                # Navegación inicial segura
                if not await navegar_seguro(page, "https://www.facebook.com/marketplace"):
                    return [], [], ["🚨 No se pudo acceder a Marketplace"]

                if "login" in page.url or "recover" in page.url:
                    return [], [], ["🚨 Sesión inválida: redirigido a login"]

                logger.info("✅ Sesión activa en Marketplace")

                modelos_shuffled = activos.copy()
                random.shuffle(modelos_shuffled)

                for i, modelo in enumerate(modelos_shuffled):
                    logger.info(f"📋 Procesando modelo {i+1}/{len(modelos_shuffled)}: {modelo}")
                    try:
                        nuevos_modelo, page = await asyncio.wait_for(
                            procesar_modelo(context, page, modelo, procesados, potenciales, relevantes), 
                            timeout=400  # Timeout más generoso
                        )
                        
                        # Pausa entre modelos
                        if i < len(modelos_shuffled) - 1:
                            await asyncio.sleep(random.uniform(10.0, 20.0))
                            
                    except asyncio.TimeoutError:
                        logger.warning(f"⏳ {modelo} → Timeout")
                        page = await recrear_pagina_si_necesario(context, page)
                    except Exception as e:
                        logger.error(f"❌ Error procesando {modelo}: {e}")
                        page = await recrear_pagina_si_necesario(context, page)

            finally:
                await browser.close()

        return procesados, potenciales, relevantes
        
    except Exception as e:
        logger.error(f"❌ Error general: {e}")
        return [], [], [f"🚨 Error general: {str(e)}"]

if __name__ == "__main__":
    async def main():
        try:
            procesados, potenciales, relevantes = await buscar_autos_marketplace()

            logger.info("📦 Resumen final")
            logger.info(f"Guardados: {len(procesados)}")
            logger.info(f"Relevantes: {len(relevantes)}")
            logger.info(f"Potenciales: {len(potenciales)}")

            for r in relevantes[:5]:
                logger.info(r.replace("*", "").replace("\\n", "\n"))
                
        except Exception as e:
            logger.error(f"❌ Error en main: {e}")

    asyncio.run(main())
