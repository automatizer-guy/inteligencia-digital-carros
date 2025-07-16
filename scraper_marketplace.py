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
    SCORE_MIN_TELEGRAM, ROI_MINIMO, es_extranjero, validar_coherencia_precio_año,
    DEBUG, analizar_mensaje
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuración optimizada
MIN_PRECIO_VALIDO = 5000
MAX_PRECIO_VALIDO = 250000
MAX_SCROLLS_POR_MODELO = 15  # Reducido para mayor eficiencia
MAX_TIMEOUT_MODELO = 340  #  minutos por modelo
MAX_REPETIDOS_CONSECUTIVOS = 3
MIN_NUEVOS_PARA_CONTINUAR = 2
ROI_POTENCIAL_MIN = ROI_MINIMO - 5

def limpiar_url(link: str) -> str:
    """Limpia y normaliza URLs de Facebook Marketplace"""
    if not link:
        return ""
    try:
        # Limpiar URL y extraer ID del item
        if "/marketplace/item/" in link:
            item_id = re.search(r"/marketplace/item/(\d+)", link)
            if item_id:
                return f"https://www.facebook.com/marketplace/item/{item_id.group(1)}"
        
        # Fallback a limpieza básica
        parsed = urlparse(link.strip())
        path = parsed.path.rstrip("/")
        return f"https://www.facebook.com{path}"
    except:
        return link.strip()

def generar_hash_contenido(texto: str) -> str:
    """Genera hash único para detectar contenido duplicado"""
    # Normalizar texto para comparación
    texto_norm = re.sub(r'\s+', ' ', texto.lower().strip())
    # Extraer características únicas (precio, año, palabras clave)
    caracteristicas = []
    
    # Extraer precio
    precio_match = re.search(r'q\s?[\d\.,]+', texto_norm)
    if precio_match:
        caracteristicas.append(precio_match.group())
    
    # Extraer año
    año_match = re.search(r'\b(19[9]\d|20[0-2]\d)\b', texto_norm)
    if año_match:
        caracteristicas.append(año_match.group())
    
    # Extraer palabras clave del modelo
    for modelo in MODELOS_INTERES:
        if modelo in texto_norm:
            caracteristicas.append(modelo)
            break
    
    return "_".join(caracteristicas)

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    """Carga contexto del navegador con cookies de Facebook"""
    logger.info("🔐 Cargando cookies desde entorno...")
    cj = os.environ.get("FB_COOKIES_JSON", "")
    
    if not cj:
        logger.warning("⚠️ Sin cookies encontradas. Usando sesión anónima.")
        return await browser.new_context(
            locale="es-ES",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
    
    try:
        cookies = json.loads(cj)
    except Exception as e:
        logger.error(f"❌ Error al parsear FB_COOKIES_JSON: {e}")
        return await browser.new_context(locale="es-ES")

    context = await browser.new_context(
        locale="es-ES",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080}
    )
    
    try:
        await context.add_cookies(cookies)
        logger.info("✅ Cookies cargadas exitosamente")
    except Exception as e:
        logger.error(f"❌ Error cargando cookies: {e}")
    
    return context

async def extraer_items_pagina(page: Page) -> List[Dict[str, str]]:
    """Extrae items de la página actual con selectores optimizados"""
    try:
        # Esperar a que carguen los elementos con timeout más corto
        await page.wait_for_selector("a[href*='/marketplace/item']", timeout=8000)
        
        # Obtener todos los enlaces de marketplace
        items_raw = await page.query_selector_all("a[href*='/marketplace/item']")
        resultados = []
        urls_vistas = set()
        
        for elemento in items_raw:
            try:
                href = await elemento.get_attribute("href")
                if not href:
                    continue
                
                url_limpia = limpiar_url(href)
                
                # Filtrar duplicados inmediatos
                if url_limpia in urls_vistas:
                    continue
                urls_vistas.add(url_limpia)
                
                # Obtener texto del elemento y contexto
                texto_elemento = await elemento.inner_text()
                aria_label = await elemento.get_attribute("aria-label") or ""
                
                # Intentar obtener más contexto del contenedor padre
                parent = await elemento.query_selector("xpath=..")
                if parent:
                    try:
                        texto_parent = await parent.inner_text()
                        # Evitar texto demasiado largo
                        if len(texto_parent) < 800:
                            texto_completo = f"{texto_elemento} {aria_label} {texto_parent}"
                        else:
                            texto_completo = f"{texto_elemento} {aria_label}"
                    except:
                        texto_completo = f"{texto_elemento} {aria_label}"
                else:
                    texto_completo = f"{texto_elemento} {aria_label}"
                
                if texto_completo.strip():
                    resultados.append({
                        "texto": texto_completo.strip()[:600],  # Limitar tamaño
                        "url": url_limpia
                    })
                    
            except Exception as e:
                if DEBUG:
                    logger.debug(f"Error procesando item: {e}")
                continue
        
        return resultados
    except Exception as e:
        logger.error(f"❌ Error extrayendo items: {e}")
        return []

async def scroll_inteligente(page: Page) -> bool:
    """Scroll más inteligente con detección de fin de contenido"""
    try:
        # Verificar altura actual
        altura_inicial = await page.evaluate("document.body.scrollHeight")
        
        # Scroll gradual más natural
        await page.evaluate("""
            window.scrollBy({
                top: window.innerHeight * 0.8,
                behavior: 'smooth'
            });
        """)
        
        # Esperar a que se cargue nuevo contenido
        await asyncio.sleep(random.uniform(1.5, 2.5))
        
        # Verificar si hay nuevo contenido
        altura_final = await page.evaluate("document.body.scrollHeight")
        
        # Verificar si estamos cerca del final
        posicion_actual = await page.evaluate("window.pageYOffset + window.innerHeight")
        cerca_del_final = posicion_actual >= altura_final * 0.95
        
        return altura_final > altura_inicial and not cerca_del_final
        
    except Exception as e:
        logger.error(f"❌ Error en scroll: {e}")
        return False

def es_anuncio_valido_rapido(texto: str, modelo: str) -> bool:
    """Filtro rápido con tolerancia mejorada para evitar descartar anuncios útiles"""
    texto_lower = texto.lower()
    
    # Verificar coincidencia más flexible
    if not coincide_modelo(texto, modelo):
        return False

    # ⚠️ No descartar por negativos: solo marcar para penalización futura
    # (mantener `contiene_negativos()` para usarlo en `puntuar_anuncio()` o `score`)
    # if contiene_negativos(texto):
    #     return False  ← esto lo quitamos para suavizar

    # 🌍 Filtrar extranjeros solo si no dice “Guatemala” explícitamente
    if es_extranjero(texto):
        return False

    # 💸 Verificar precios válidos
    precios = re.findall(r'q\s?[\d\.,]+', texto_lower)
    for precio_str in precios:
        precio_num = re.sub(r'[^\d]', '', precio_str)
        if precio_num.isdigit():
            precio = int(precio_num)
            if MIN_PRECIO_VALIDO <= precio <= MAX_PRECIO_VALIDO:
                return True

    # ⛔ Si no hay ningún precio válido, sí lo descartamos
    return False


async def procesar_modelo_optimizado(page: Page, modelo: str,
                                   procesados: List[str],
                                   potenciales: List[str],
                                   relevantes: List[str]) -> Dict[str, int]:
    """Procesa un modelo con optimizaciones críticas aplicadas"""
    
    # Contadores detallados
    stats = {
        "encontrados": 0,
        "duplicados_url": 0,
        "duplicados_contenido": 0,
        "filtro_rapido": 0,
        "datos_incompletos": 0,
        "precio_incoherente": 0,
        "guardados": 0,
        "relevantes": 0,
        "potenciales": 0,
        "tiempo_inicio": datetime.now()
    }
    
    # Sets para control de duplicados
    urls_vistas = set()
    contenido_visto = set()
    
    # Estrategia de búsqueda más eficiente
    ORDENAMIENTOS = ["newest", "price_asc"]  # Solo 2 ordenamientos
    
    logger.info(f"🔍 Iniciando procesamiento: {modelo.upper()}")
    
    for idx, sort_param in enumerate(ORDENAMIENTOS):
        logger.info(f"📊 {modelo} - Ordenamiento {idx+1}/{len(ORDENAMIENTOS)}: {sort_param}")
        
        # URL de búsqueda optimizada
        url_busqueda = (
            f"https://www.facebook.com/marketplace/guatemala/search/"
            f"?query={modelo.replace(' ', '%20')}"
            f"&minPrice=5000&maxPrice=200000"
            f"&sortBy={sort_param}"
            f"&exact=false"
        )
        
        try:
            await page.goto(url_busqueda, timeout=25000)
            await asyncio.sleep(random.uniform(2, 3))
        except Exception as e:
            logger.warning(f"⚠️ Error navegando para {modelo}: {e}")
            continue
        
        # Variables de control de scroll
        scrolls_realizados = 0
        sin_nuevos_consecutivos = 0
        items_en_ordenamiento = 0
        
        while (scrolls_realizados < MAX_SCROLLS_POR_MODELO and 
               sin_nuevos_consecutivos < MAX_REPETIDOS_CONSECUTIVOS):
            
            # Extraer items de la página actual
            items = await extraer_items_pagina(page)
            stats["encontrados"] += len(items)
            
            nuevos_en_scroll = 0
            
            for item in items:
                url = item["url"]
                texto = item["texto"]
            
                # Filtro de URL duplicada
                if url in urls_vistas:
                    stats["duplicados_url"] += 1
                    continue
                urls_vistas.add(url)
            
                # Filtro de contenido duplicado
                hash_contenido = generar_hash_contenido(texto)
                if hash_contenido in contenido_visto:
                    stats["duplicados_contenido"] += 1
                    continue
                contenido_visto.add(hash_contenido)
            
                # Filtro rápido inicial
                if not es_anuncio_valido_rapido(texto, modelo):
                    stats["filtro_rapido"] += 1
                    continue
            
                # ✅ Entrar al anuncio y extraer descripción
                try:
                    await page.goto(url, timeout=10000)
                    await asyncio.sleep(random.uniform(1.5, 2.5))  # Pausa natural
                    descripcion = ""
                    try:
                        await page.wait_for_selector('div[aria-label="Descripción"]', timeout=4000)
                        descripcion = await page.inner_text('div[aria-label="Descripción"]')
                    except:
                        pass
                except Exception as e:
                    logger.warning(f"⚠️ No se pudo acceder al anuncio: {e}")
                    descripcion = ""
            
                # 🧠 Análisis completo con descripción incluida
                resultado = analizar_mensaje(f"{texto} {descripcion} {url}")
                if not resultado:
                    stats["datos_incompletos"] += 1
                    continue
            
                # Verificar coincidencia de modelo exacto
                if resultado["modelo"] != modelo:
                    continue
            
                precio = resultado["precio"]
                anio = resultado["año"]
                roi = resultado["roi"]
                score = resultado["score"]
                es_relevante = resultado["relevante"]
            
                # 🧾 Formar mensaje
                mensaje = (
                    f"🚗 *{modelo.title()}*\n"
                    f"• Año: {anio}\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• ROI: {roi:.1f}%\n"
                    f"• Score: {score}/10\n"
                    f"🔗 {url}"
                )
            
                # 🧠 Guardar en base
                try:
                    insertar_anuncio_db(
                        link=url,
                        modelo=modelo,
                        anio=anio,
                        precio=precio,
                        km="",
                        roi=roi,
                        score=score,
                        relevante=es_relevante,
                        confianza_precio=resultado["confianza_precio"],
                        muestra_precio=resultado["muestra_precio"]
                        # Podés agregar `descripcion=descripcion` si tenés esa columna
                    )
                    stats["guardados"] += 1
                    nuevos_en_scroll += 1
                    items_en_ordenamiento += 1
                except Exception as e:
                    logger.error(f"❌ Error guardando {url}: {e}")
                    continue
            
                # Agregar a listas
                procesados.append(mensaje)
                if es_relevante:
                    relevantes.append(mensaje)
                    stats["relevantes"] += 1
                    logger.info(f"✅ RELEVANTE: {modelo} {anio} Q{precio:,} ROI: {roi:.1f}%")
                elif roi >= ROI_POTENCIAL_MIN:
                    potenciales.append(mensaje)
                    stats["potenciales"] += 1
                    logger.info(f"🟡 POTENCIAL: {modelo} {anio} Q{precio:,} ROI: {roi:.1f}%")
                else:
                    logger.info(f"💾 GUARDADO: {modelo} {anio} Q{precio:,} ROI: {roi:.1f}%")

            
            # Control de scroll
            scrolls_realizados += 1
            
            if nuevos_en_scroll == 0:
                sin_nuevos_consecutivos += 1
                logger.info(f"⚠️ {modelo}: Sin nuevos en scroll {scrolls_realizados} (consecutivos: {sin_nuevos_consecutivos})")
            else:
                sin_nuevos_consecutivos = 0
                logger.info(f"📊 {modelo}: {nuevos_en_scroll} nuevos en scroll {scrolls_realizados}")
            
            # Límite de items por ordenamiento
            if items_en_ordenamiento >= 40:
                logger.info(f"⏹️ {modelo}: Límite de items alcanzado ({items_en_ordenamiento})")
                break
            
            # Intentar scroll
            if not await scroll_inteligente(page):
                logger.info(f"⏹️ {modelo}: Fin de contenido detectado")
                break
            
            # Pausa entre scrolls
            await asyncio.sleep(random.uniform(1, 1.5))
        
        # Reporte por ordenamiento
        logger.info(f"📋 {modelo} - {sort_param}: {items_en_ordenamiento} items procesados")
    
    # Estadísticas finales del modelo
    duracion = (datetime.now() - stats["tiempo_inicio"]).seconds
    eficiencia = (stats["relevantes"] / stats["encontrados"] * 100) if stats["encontrados"] > 0 else 0
    
    logger.info(f"""
🎯 MODELO TERMINADO: {modelo.upper()}
   ⏱️ Tiempo: {duracion}s
   📊 Encontrados: {stats['encontrados']}
   🔄 Duplicados URL: {stats['duplicados_url']}
   🔄 Duplicados contenido: {stats['duplicados_contenido']}
   🚫 Filtro rápido: {stats['filtro_rapido']}
   ❌ Datos incompletos: {stats['datos_incompletos']}
   💾 Guardados: {stats['guardados']}
   🎯 Relevantes: {stats['relevantes']}
   🟡 Potenciales: {stats['potenciales']}
   📈 Eficiencia: {eficiencia:.1f}%
🎯""")
    
    return stats

async def buscar_autos_marketplace(modelos_override: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
    """Función principal optimizada para buscar autos en Facebook Marketplace"""
    logger.info("🚀 Iniciando scraper OPTIMIZADO de Facebook Marketplace")
    
    # Inicializar base de datos
    inicializar_tabla_anuncios()
    
    # Determinar modelos a procesar
    modelos_a_procesar = modelos_override or MODELOS_INTERES
    modelos_pausados = modelos_bajo_rendimiento()
    modelos_activos = [m for m in modelos_a_procesar if m not in modelos_pausados]
    
    if not modelos_activos:
        logger.warning("⚠️ No hay modelos activos para procesar")
        return [], [], ["⚠️ No hay modelos activos para procesar"]
    
    logger.info(f"📋 Modelos activos ({len(modelos_activos)}): {modelos_activos}")
    if modelos_pausados:
        logger.info(f"⏸️ Modelos pausados ({len(modelos_pausados)}): {modelos_pausados}")
    
    # Listas de resultados
    todos_procesados = []
    todos_potenciales = []
    todos_relevantes = []
    alertas = []
    
    # Estadísticas globales
    stats_globales = {
        "modelos_procesados": 0,
        "total_encontrados": 0,
        "total_guardados": 0,
        "total_relevantes": 0,
        "total_potenciales": 0,
        "tiempo_inicio": datetime.now()
    }
    
    # Iniciar navegador
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-first-run',
                '--no-default-browser-check',
                '--disable-extensions'
            ]
        )
        
        try:
            # Cargar contexto con cookies
            context = await cargar_contexto_con_cookies(browser)
            page = await context.new_page()
            
            # Verificar acceso básico
            try:
                await page.goto("https://www.facebook.com/marketplace/guatemala", timeout=20000)
                await asyncio.sleep(2)
                
                # Verificar si estamos bloqueados
                if "checkpoint" in page.url or "login" in page.url:
                    alertas.append("❌ Sesión de Facebook expirada o bloqueada")
                    return [], [], alertas
                
            except Exception as e:
                logger.error(f"❌ Error accediendo a Marketplace: {e}")
                alertas.append(f"❌ Error de conexión: {str(e)[:100]}")
                return [], [], alertas
            
            # Procesar cada modelo
            for i, modelo in enumerate(modelos_activos, 1):
                logger.info(f"🔄 Procesando modelo {i}/{len(modelos_activos)}: {modelo}")
                
                try:
                    # Procesar modelo con timeout
                    stats_modelo = await asyncio.wait_for(
                        procesar_modelo_optimizado(page, modelo, todos_procesados, todos_potenciales, todos_relevantes),
                        timeout=MAX_TIMEOUT_MODELO
                    )
                    
                    # Actualizar estadísticas globales
                    stats_globales["modelos_procesados"] += 1
                    stats_globales["total_encontrados"] += stats_modelo["encontrados"]
                    stats_globales["total_guardados"] += stats_modelo["guardados"]
                    stats_globales["total_relevantes"] += stats_modelo["relevantes"]
                    stats_globales["total_potenciales"] += stats_modelo["potenciales"]
                    
                    # Pausa entre modelos
                    if i < len(modelos_activos):
                        await asyncio.sleep(random.uniform(2, 4))
                    
                except asyncio.TimeoutError:
                    logger.warning(f"⏰ Timeout procesando {modelo}")
                    alertas.append(f"⏰ Timeout procesando {modelo}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Error procesando {modelo}: {e}")
                    alertas.append(f"❌ Error en {modelo}: {str(e)[:100]}")
                    continue
            
        except Exception as e:
            logger.error(f"❌ Error general del scraper: {e}")
            alertas.append(f"❌ Error general: {str(e)[:100]}")
        
        finally:
            await browser.close()
    
    # Reporte final
    duracion_total = (datetime.now() - stats_globales["tiempo_inicio"]).seconds
    eficiencia_global = (stats_globales["total_relevantes"] / stats_globales["total_encontrados"] * 100) if stats_globales["total_encontrados"] > 0 else 0
    
    logger.info(f"""
🎉 SCRAPING COMPLETADO
   ⏱️ Tiempo total: {duracion_total}s ({duracion_total/60:.1f}min)
   📊 Modelos procesados: {stats_globales['modelos_procesados']}/{len(modelos_activos)}
   🔍 Total encontrados: {stats_globales['total_encontrados']}
   💾 Total guardados: {stats_globales['total_guardados']}
   🎯 Total relevantes: {stats_globales['total_relevantes']}
   🟡 Total potenciales: {stats_globales['total_potenciales']}
   📈 Eficiencia global: {eficiencia_global:.1f}%
   🚨 Alertas: {len(alertas)}
🎉""")
    
    return todos_procesados, todos_potenciales, alertas

# Punto de entrada para testing
if __name__ == "__main__":
    async def main():
        resultados = await buscar_autos_marketplace()
        print(f"Procesados: {len(resultados[0])}")
        print(f"Potenciales: {len(resultados[1])}")
        print(f"Alertas: {len(resultados[2])}")
    
    asyncio.run(main())
