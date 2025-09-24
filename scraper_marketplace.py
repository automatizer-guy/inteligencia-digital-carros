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

# Configuración CONSERVADORA - Solo cambios mínimos
MAX_SCROLLS_POR_SORT = 15  # ORIGINAL - Sin cambios
MIN_DELAY = 2.5  # +0.5 segundos SOLAMENTE
MAX_DELAY = 4.5  # +0.5 segundos SOLAMENTE  
DELAY_ENTRE_ANUNCIOS = 2.3  # +0.3 segundos SOLAMENTE
MAX_CONSECUTIVOS_SIN_NUEVOS = 3  # ORIGINAL
BATCH_SIZE_SCROLL = 8  # ORIGINAL

# ÚNICA MEJORA IMPORTANTE: User-agents rotativos
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",  # Original
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
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

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    """Carga el contexto del browser con cookies de Facebook - MÍNIMA MEJORA"""
    logger.info("🔐 Cargando cookies desde entorno…")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
        return await browser.new_context(locale="es-ES")
    
    try:
        cookies = json.loads(cj)
        context = await browser.new_context(
            locale="es-ES",
            user_agent=random.choice(USER_AGENTS)  # ÚNICA MEJORA AQUÍ
        )
        await context.add_cookies(cookies)
        return context
    except Exception as e:
        logger.error(f"❌ Error al parsear FB_COOKIES_JSON: {e}")
        return await browser.new_context(locale="es-ES")

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extrae items de anuncios de la página actual"""
    try:
        items = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        for a in items:
            try:
                titulo = (await a.inner_text()).strip()
                aria_label = await a.get_attribute("aria-label") or ""
                texto_completo = f"{titulo} {aria_label}".strip()
                href = await a.get_attribute("href") or ""
                resultados.append({"texto": texto_completo, "url": limpiar_url(href)})
            except Exception as e:
                logger.warning(f"Error extrayendo item individual: {e}")
                continue
        return resultados
    except Exception as e:
        logger.error(f"❌ Error al extraer items de página: {e}")
        return []

async def scroll_hasta(page: Page) -> bool:
    """Realiza scroll simulando comportamiento humano - MEJORA MÍNIMA"""
    try:
        # Simular movimiento de mouse humano antes del scroll
        await page.mouse.move(
            random.randint(100, 800),
            random.randint(100, 600),
            steps=random.randint(3, 8)  # ÚNICA MEJORA: movimiento más gradual
        )
        await asyncio.sleep(random.uniform(0.5, 1.2))

        # Evaluar altura inicial de la página
        prev = await page.evaluate("document.body.scrollHeight")

        # Scroll más suave y realista
        await page.mouse.wheel(0, random.randint(180, 350))  # Rango ligeramente más amplio
        await asyncio.sleep(random.uniform(1.5, 2.5))  # ORIGINAL

        # Evaluar nueva altura de la página
        now = await page.evaluate("document.body.scrollHeight")

        return now > prev
    except Exception as e:
        logger.warning(f"Error durante scroll: {e}")
        return False

async def extraer_texto_anuncio(page: Page, url: str) -> str:
    """Extrae texto del anuncio con múltiples estrategias de fallback"""
    texto = "Sin texto disponible"
    
    try:
        # Estrategia 1: Contenido principal
        try:
            texto_extraido = await asyncio.wait_for(
                page.inner_text("div[role='main']"), 
                timeout=10
            )
            if texto_extraido and len(texto_extraido.strip()) >= 100:
                return texto_extraido.strip()
        except Exception:
            pass

        # Estrategia 2: Título de la página
        try:
            texto_title = await page.title()
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

        # Estrategia 4: Contenido del body (último recurso)
        try:
            if texto == "Sin texto disponible":
                body_text = await page.inner_text("body")
                if body_text and len(body_text.strip()) > 50:
                    texto = body_text.strip()[:500]
        except Exception:
            pass

    except Exception as e:
        logger.warning(f"Error general extrayendo texto de {url}: {e}")
    
    return texto

async def procesar_lote_urls(
    page: Page,
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
        texto = "Sin texto disponible"
        procesado_exitosamente = False
        
        if url in vistos_globales:
            contador["duplicado"] += 1
            continue
        vistos_globales.add(url)

        try:
            await asyncio.wait_for(page.goto(url, wait_until='domcontentloaded'), timeout=15)
            await asyncio.sleep(DELAY_ENTRE_ANUNCIOS)

            texto = await extraer_texto_anuncio(page, url)
            
            if len(texto.strip()) < 10:
                logger.warning(f"Texto insuficiente para {url}: '{texto[:50]}...'")
                contador["texto_insuficiente"] = contador.get("texto_insuficiente", 0) + 1
                continue
                
            procesado_exitosamente = True

        except asyncio.TimeoutError:
            logger.warning(f"Timeout procesando URL {url}")
            contador["timeout"] = contador.get("timeout", 0) + 1
            continue
        except Exception as e:
            logger.warning(f"Error procesando URL {url}: {e}")
            contador["error"] = contador.get("error", 0) + 1
            continue

        if not procesado_exitosamente:
            continue

        try:
            if await procesar_anuncio_individual(
                page, url, texto, modelo, contador,
                procesados, potenciales, relevantes, sin_anio_ejemplos
            ):
                nuevos_en_lote += 1
                
                # Pausa cada 3 anuncios procesados exitosamente - ORIGINAL
                if nuevos_en_lote % 3 == 0:
                    await asyncio.sleep(random.uniform(2.0, 3.5))
        except Exception as e:
            logger.error(f"Error en procesar_anuncio_individual para {url}: {e}")
            contador["error_procesamiento"] = contador.get("error_procesamiento", 0) + 1
            continue
    
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

        # Intento expandir descripción solo si no hay año válido
        if not anio or not (1990 <= anio <= datetime.now().year):
            try:
                ver_mas = await page.query_selector("div[role='main'] span:has-text('Ver más')")
                if ver_mas:
                    await ver_mas.click()
                    await asyncio.sleep(1.5)
                    texto_expandido = await page.inner_text("div[role='main']")
                    anio_expandido = extraer_anio(texto_expandido)
                    if anio_expandido and (1990 <= anio_expandido <= datetime.now().year):
                        anio = anio_expandido
                        texto = texto_expandido
            except Exception as e:
                logger.warning(f"Error al expandir descripción: {e}")
        
        # Validación final del año
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
                logger.info(f"💾 Guardado nuevo: {modelo} | ROI={roi_data['roi']:.2f}% | Score={score} | Relevante={relevante}")
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
        logger.error(f"Error general en procesar_anuncio_individual: {e}")
        contador["error_general"] = contador.get("error_general", 0) + 1
        return False

async def procesar_ordenamiento_optimizado(page: Page, modelo: str, sort: str, 
                                         vistos_globales: Set[str], contador: Dict[str, int],
                                         procesados: List[str], potenciales: List[str], 
                                         relevantes: List[str], sin_anio_ejemplos: List[Tuple[str, str]]) -> int:
    """Versión optimizada del procesamiento por ordenamiento"""
    
    try:
        url_busq = f"https://www.facebook.com/marketplace/guatemala/search/?query={modelo.replace(' ', '%20')}&minPrice=1000&maxPrice=60000&sortBy={sort}"
        await page.goto(url_busq, wait_until='domcontentloaded')
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        scrolls_realizados = 0
        consec_repetidos = 0
        nuevos_total = 0
        urls_pendientes = []

        while scrolls_realizados < MAX_SCROLLS_POR_SORT:
            # Extraer URLs en lote
            try:
                items = await extraer_items_pagina(page)
                urls_nuevas = []
                
                for itm in items:
                    url = limpiar_link(itm["url"])
                    contador["total"] += 1

                    if not url or not url.startswith("https://www.facebook.com/marketplace/item/"):
                        continue
                        
                    if url not in vistos_globales:
                        urls_nuevas.append(url)

                urls_pendientes.extend(urls_nuevas)
                
                # Procesar en lotes cuando tengamos suficientes URLs o al final
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

            except Exception as e:
                logger.warning(f"Error extrayendo items en scroll {scrolls_realizados}: {e}")

            scrolls_realizados += 1
            
            # Salida temprana optimizada
            if consec_repetidos >= MAX_CONSECUTIVOS_SIN_NUEVOS and len(urls_nuevas) < 2:
                logger.info(f"🔄 Salida temprana en {sort}: {consec_repetidos} scrolls consecutivos sin nuevos")
                break
                
            if not await scroll_hasta(page):
                logger.info(f"🔄 Fin de contenido detectado en {sort}")
                break

        # Procesar URLs restantes
        if urls_pendientes:
            await procesar_lote_urls(page, urls_pendientes, modelo, vistos_globales, 
                                   contador, procesados, potenciales, relevantes, sin_anio_ejemplos)

        return nuevos_total
        
    except Exception as e:
        logger.error(f"Error en procesar_ordenamiento_optimizado: {e}")
        return 0

async def procesar_modelo(page: Page, modelo: str,
                          procesados: List[str],
                          potenciales: List[str],
                          relevantes: List[str]) -> int:
    """Procesa un modelo específico con todos los ordenamientos"""
    
    vistos_globales = set()
    sin_anio_ejemplos = []
    contador = {k: 0 for k in [
        "total", "duplicado", "negativo", "sin_precio", "sin_anio",
        "filtro_modelo", "guardado", "precio_bajo", "extranjero",
        "actualizados", "repetidos", "error", "timeout", "texto_insuficiente",
        "error_procesamiento", "error_db", "error_general", "texto_vacio"
    ]}
    
    # Ordenamientos ORIGINALES - sin cambios para mantener velocidad
    SORT_OPTS = ["best_match", "price_asc"]
    inicio = datetime.now()
    total_nuevos = 0

    for sort in SORT_OPTS:
        logger.info(f"🔍 Procesando {modelo} con ordenamiento: {sort}")
        try:
            nuevos_sort = await asyncio.wait_for(
                procesar_ordenamiento_optimizado(page, modelo, sort, vistos_globales, contador,
                                                procesados, potenciales, relevantes, sin_anio_ejemplos), 
                timeout=180  # ORIGINAL - 3 minutos por ordenamiento
            )
            total_nuevos += nuevos_sort
            logger.info(f"✅ {sort}: {nuevos_sort} nuevos anuncios procesados")
            
            # Pausa entre ordenamientos - ORIGINAL
            if sort != SORT_OPTS[-1]:
                await asyncio.sleep(random.uniform(3.0, 5.0))
            
        except asyncio.TimeoutError:
            logger.warning(f"⏳ Timeout en ordenamiento {sort} para {modelo}")
            continue
        except Exception as e:
            logger.error(f"❌ Error en ordenamiento {sort} para {modelo}: {e}")
            continue

    duracion = (datetime.now() - inicio).seconds
    
    # Log detallado con todos los contadores
    logger.info(f"""
✨ MODELO: {modelo.upper()} - OPTIMIZADO
   Duración: {duracion} s
   Total encontrados: {contador['total']}
   Guardados nuevos: {contador['guardado']}
   Actualizados: {contador.get('actualizados', 0)}
   Repetidos sin cambios: {contador.get('repetidos', 0)}
   Relevantes: {len([r for r in relevantes if modelo.lower() in r.lower()])}
   Potenciales: {len([p for p in potenciales if modelo.lower() in p.lower()])}
   
   Filtrados por:
   - Duplicados: {contador['duplicado']}
   - Modelo no coincide: {contador['filtro_modelo']}
   - Precio bajo: {contador['precio_bajo']}
   - Sin año: {contador['sin_anio']}
   - Negativos: {contador['negativo']}
   - Extranjero: {contador['extranjero']}
   - Sin precio: {contador['sin_precio']}
   
   Errores:
   - Errores de carga: {contador.get('error', 0)}
   - Timeouts: {contador.get('timeout', 0)}
   - Texto insuficiente: {contador.get('texto_insuficiente', 0)}
   - Error procesamiento: {contador.get('error_procesamiento', 0)}
   - Error DB: {contador.get('error_db', 0)}
   - Error general: {contador.get('error_general', 0)}
   - Parámetros inválidos: {contador.get('parametros_invalidos', 0)}
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
            logger.warning("⚠️ No hay modelos activos por rendimiento. Usando todos los modelos por defecto.")
            activos = modelos


        procesados, potenciales, relevantes = [], [], []

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled']
            )
            
            try:
                ctx = await cargar_contexto_con_cookies(browser)
                page = await ctx.new_page()

                # Configuración optimizada de la página - ORIGINAL
                await page.set_extra_http_headers({
                    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
                })

                await page.goto("https://www.facebook.com/marketplace", wait_until='domcontentloaded')
                await asyncio.sleep(3)  # ORIGINAL

                if "login" in page.url or "recover" in page.url:
                    alerta = "🚨 Sesión inválida: redirigido a la página de inicio de sesión. Verifica las cookies (FB_COOKIES_JSON)."
                    logger.warning(alerta)
                    return [], [], [alerta]

                logger.info("✅ Sesión activa detectada correctamente en Marketplace.")

                # Aleatorizar el orden pero mantener determinismo para logs
                modelos_shuffled = activos.copy()
                random.shuffle(modelos_shuffled)

                for i, m in enumerate(modelos_shuffled):
                    logger.info(f"📋 Procesando modelo {i+1}/{len(modelos_shuffled)}: {m}")
                    try:
                        await asyncio.wait_for(
                            procesar_modelo(page, m, procesados, potenciales, relevantes), 
                            timeout=360  # ORIGINAL - 6 minutos por modelo
                        )
                        
                        # Pausa entre modelos - SOLO LIGERAMENTE AUMENTADA
                        if i < len(modelos_shuffled) - 1:
                            await asyncio.sleep(random.uniform(8.0, 18.0))  # +3s máximo vs original
                            
                    except asyncio.TimeoutError:
                        logger.warning(f"⏳ {m} → Excedió tiempo máximo. Se aborta.")
                    except Exception as e:
                        logger.error(f"❌ Error procesando modelo {m}: {e}")

            finally:
                await browser.close()

        return procesados, potenciales, relevantes
        
    except Exception as e:
        logger.error(f"❌ Error general en buscar_autos_marketplace: {e}")
        return [], [], [f"🚨 Error general: {str(e)}"]

if __name__ == "__main__":
    async def main():
        try:
            procesados, potenciales, relevantes = await buscar_autos_marketplace()

            logger.info("📦 Resumen final del scraping optimizado")
            logger.info(f"Guardados totales: {len(procesados)}")
            logger.info(f"Relevantes: {len(relevantes)}")
            logger.info(f"Potenciales: {len(potenciales)}")

            logger.info("\n🟢 Relevantes con buen ROI:")
            for r in relevantes[:10]:  # Limitar output para logging
                logger.info(r.replace("*", "").replace("\\n", "\n"))
                
        except Exception as e:
            logger.error(f"❌ Error en main: {e}")

    asyncio.run(main())
