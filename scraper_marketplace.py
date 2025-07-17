import os
import re
import json
import random
import asyncio
import logging
from urllib.parse import urlparse
from typing import List, Dict, Optional, Any, Tuple
from playwright.async_api import async_playwright, Browser, Page, BrowserContext

# Importaciones desde utils_analisis
from utils_analisis import (
    limpiar_precio,
    contiene_negativos,
    puntuar_anuncio,
    calcular_roi_real,
    coincide_modelo,
    extraer_anio,
    inicializar_tabla_anuncios,
    limpiar_link,
    modelos_bajo_rendimiento,
    MODELOS_INTERES,
    Config,
    validar_anuncio_completo,
    insertar_anuncio_en_db
)

# --- Configuración de logging ---
# El logger se obtiene por nombre para permitir configuración granular.
logger = logging.getLogger(__name__)
# Configuración básica de logging para la consola.
# El nivel se ajusta a INFO, pero podría ser configurable por Config.DEBUG.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --- Sincronizar configuración ---
# Asegura que la ruta de la base de datos sea la misma en ambos módulos.
DB_PATH = os.path.abspath(os.environ.get("DB_PATH", "upload-artifact/anuncios.db"))
Config.DB_PATH = DB_PATH

# Constantes para lógica de negocio
MIN_PRECIO_VALIDO: int = 3000 # Definido en el código original, pero Config.PRECIO_MIN_VALIDO es 5000.
# Considerar si esta constante debería unificarse o si tiene un propósito diferente.
# Se mantiene para no alterar la lógica existente.
ROI_POTENCIAL_MIN: float = Config.ROI_MINIMO - 10

# --- Funciones de utilidad ---

def limpiar_url(link: str) -> str:
    """
    Normaliza URLs de Facebook Marketplace eliminando parámetros de consulta
    y asegurando un formato base consistente.

    Args:
        link (str): El enlace URL a normalizar.

    Returns:
        str: El enlace URL normalizado.
    """
    if not link:
        return ""
    # Parsear la URL para obtener solo la ruta y luego limpiar espacios y slashes finales.
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    """
    Configura un nuevo contexto de navegador y carga cookies desde un archivo JSON
    para mantener sesiones de usuario (ej. sesión de Facebook).

    Args:
        browser (Browser): La instancia del navegador Playwright.

    Returns:
        BrowserContext: El contexto del navegador con las cookies cargadas (si existen).
    """
    context = await browser.new_context()
    cookies_path = "cookies_marketplace.json"
    if os.path.exists(cookies_path):
        try:
            with open(cookies_path, "r", encoding="utf-8") as f:
                cookies = json.load(f)
                await context.add_cookies(cookies)
                logger.info("Cookies cargadas correctamente.")
        except json.JSONDecodeError:
            logger.warning(f"Error: El archivo de cookies '{cookies_path}' no es un JSON válido.")
        except Exception as e:
            logger.warning(f"Error cargando cookies desde '{cookies_path}': {e}")
    return context

async def extraer_enlaces(page: Page, modelo: str, max_scrolls: int = 12) -> List[str]:
    """
    Extrae enlaces de anuncios de una página de búsqueda de Facebook Marketplace
    realizando scrolls para cargar contenido dinámico.

    Args:
        page (Page): La instancia de la página Playwright.
        modelo (str): El modelo de vehículo que se está buscando.
        max_scrolls (int): El número máximo de veces que se realizará scroll.

    Returns:
        List[str]: Una lista de enlaces de anuncios únicos y normalizados.
    """
    enlaces = set()
    contador_scroll = 0
    
    logger.info(f"Iniciando extracción de enlaces para '{modelo}'.")
    
    # Pre-compilar el patrón de regex para los enlaces para mayor eficiencia.
    link_pattern = re.compile(r'/marketplace/item/')

    while contador_scroll < max_scrolls:
        try:
            # Desplazarse al final de la página para cargar más contenido.
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            # Espera aleatoria para simular comportamiento humano y permitir la carga de contenido.
            await asyncio.sleep(random.uniform(2, 4))
            
            # Extraer nuevos enlaces que contengan el patrón '/marketplace/item/'.
            # Usar 'page.locator' es a menudo más robusto que 'query_selector_all' en Playwright.
            nuevos_elementos = await page.locator("a").filter(has=page.locator(f"a[href*='{link_pattern.pattern}']")).all()
            
            for elemento in nuevos_elementos:
                href = await elemento.get_attribute("href")
                if href:
                    enlaces.add(limpiar_url(href))
            
            contador_scroll += 1
            logger.info(f"Scroll {contador_scroll}/{max_scrolls} - Enlaces encontrados: {len(enlaces)}")
            
        except Exception as e:
            logger.error(f"Error durante el scroll y extracción de enlaces para '{modelo}': {e}")
            break # Romper el bucle si ocurre un error grave durante el scroll.
            
    return list(enlaces)

# Pre-compilar patrones de regex fuera de la función para mejor rendimiento.
PATRONES_EXTRACCION_DATOS = {
    "precio_texto": re.compile(r"(Q[\d\.,]+|\$[\d\.,]+)"),
    "titulo": re.compile(r"^(.*?)(?:\n|$)", re.MULTILINE | re.DOTALL),
    "descripcion": re.compile(r"Descripción\n(.*?)(?:\n[A-Z]|$)", re.MULTILINE | re.IGNORECASE | re.DOTALL),
    "ubicacion": re.compile(r"Ubicación\n(.*?)(?:\n|$)", re.MULTILINE | re.IGNORECASE | re.DOTALL),
    "kilometraje": re.compile(r"Kilometraje\n(.*?)(?:\n|$)", re.MULTILINE | re.IGNORECASE | re.DOTALL)
}

async def extraer_datos_del_anuncio(page: Page) -> Dict[str, str]:
    """
    Extrae datos estructurados de la página de un anuncio de Facebook Marketplace.

    Args:
        page (Page): La instancia de la página Playwright del anuncio.

    Returns:
        Dict[str, str]: Un diccionario con los datos extraídos (título, descripción, precio, etc.).
                       Retorna un diccionario vacío si la extracción falla.
    """
    try:
        await asyncio.sleep(random.uniform(1, 3)) # Espera aleatoria antes de extraer datos.
        
        # Intentar localizar el contenedor principal del contenido del anuncio.
        # Esto es más específico que solo 'div[aria-label="Contenido de la página"]'.
        # Se puede intentar con múltiples selectores si uno falla.
        elemento_contenido = await page.wait_for_selector("div[role='main'] div[aria-label='Contenido de la página'], div[data-pagelet='MarketplaceItemPage']", timeout=10000)
        
        if not elemento_contenido:
            logger.warning("No se encontró el elemento de contenido principal del anuncio.")
            return {}
        
        # Obtener todo el texto dentro del elemento de contenido.
        texto_total = await elemento_contenido.inner_text()
        
        datos: Dict[str, str] = {}
        datos["texto_crudo"] = texto_total # Siempre guardar el texto original.

        for campo, patron in PATRONES_EXTRACCION_DATOS.items():
            match = patron.search(texto_total)
            datos[campo] = match.group(1).strip() if match else ""

        return datos
            
    except Exception as e:
        logger.error(f"Error extrayendo datos del anuncio en {page.url}: {e}")
        return {}

async def analizar_enlace(context: BrowserContext, link: str, modelo: str) -> Optional[Dict[str, Any]]:
    """
    Navega a un enlace de anuncio, extrae sus datos, los valida y calcula su ROI y puntuación.

    Args:
        context (BrowserContext): El contexto del navegador Playwright.
        link (str): El enlace del anuncio a analizar.
        modelo (str): El modelo de vehículo asociado a la búsqueda original.

    Returns:
        Optional[Dict[str, Any]]: Un diccionario con los datos analizados del anuncio,
                                   o None si el análisis falla.
    """
    page: Optional[Page] = None
    try:
        page = await context.new_page()
        # Navegar a la página del anuncio con un timeout.
        await page.goto(link, timeout=60000)
        
        # Esperar a que el contenido crítico sea visible, si es posible.
        # Por ejemplo, esperar a que el título o precio aparezcan.
        await page.wait_for_selector("div[aria-label='Contenido de la página'], h1", timeout=15000)

        datos_extraidos = await extraer_datos_del_anuncio(page)
        
        if not datos_extraidos:
            logger.warning(f"No se pudieron extraer datos del anuncio: {link}")
            return None

        # Procesar datos básicos
        precio = limpiar_precio(datos_extraidos.get("precio_texto", ""))
        texto_combinado = f"{datos_extraidos.get('titulo', '')} {datos_extraidos.get('descripcion', '')}"
        anio = extraer_anio(texto_combinado)
        km = datos_extraidos.get("kilometraje", "")

        # Estructura base del anuncio a retornar
        anuncio: Dict[str, Any] = {
            "link": limpiar_link(link),
            "precio": precio,
            "modelo": modelo, # Modelo de la búsqueda, no necesariamente el extraído del texto.
            "anio": anio,
            "km": km,
            "descripcion": datos_extraidos.get("descripcion", ""),
            "texto_crudo": datos_extraidos.get("texto_crudo", ""),
            "ubicacion": datos_extraidos.get("ubicacion", ""),
            "score": 0,
            "roi": 0.0,
            "motivo": "", # Razón por la cual fue aceptado/descartado.
            "confianza_precio": "baja",
            "muestra_precio": 0,
            "relevante": 0 # Booleano (0 o 1) para si es relevante para Telegram.
        }

        # --- Validación inicial y filtros ---
        # Usa la función centralizada de validación.
        valido, motivo_rechazo = validar_anuncio_completo(
            texto=anuncio["texto_crudo"],
            precio=precio,
            anio=anio,
            modelo=modelo
        )
        
        if not valido:
            anuncio["motivo"] = motivo_rechazo
            logger.debug(f"Anuncio '{link}' descartado por validación inicial: {motivo_rechazo}")
            return anuncio

        # Filtros adicionales basados en contenido
        if contiene_negativos(anuncio["texto_crudo"].lower()): # Asegurarse de pasar el texto completo en minúsculas
            anuncio["motivo"] = "contiene_palabras_negativas"
            logger.debug(f"Anuncio '{link}' descartado: {anuncio['motivo']}")
            return anuncio

        if not coincide_modelo(anuncio["texto_crudo"].lower(), modelo):
            anuncio["motivo"] = "no_coincide_modelo"
            logger.debug(f"Anuncio '{link}' descartado: {anuncio['motivo']}")
            return anuncio

        # --- Cálculo de ROI y puntuación ---
        roi_data = calcular_roi_real(modelo, precio, anio)
        score = puntuar_anuncio(anuncio["texto_crudo"], roi_data)

        # Actualizar anuncio con datos calculados
        anuncio.update({
            "score": score,
            "roi": roi_data["roi"],
            "confianza_precio": roi_data["confianza"],
            "muestra_precio": roi_data["muestra"],
            # 'relevante' es 1 si cumple ambos criterios, 0 en caso contrario.
            "relevante": 1 if score >= Config.SCORE_MIN_TELEGRAM and roi_data["roi"] >= Config.ROI_MINIMO else 0
        })

        # --- Determinar motivo final ---
        # Este 'motivo' se usará para el logging y puede ser útil en la DB.
        if anuncio["relevante"] == 1:
            anuncio["motivo"] = "candidato_valido"
        elif score < Config.SCORE_MIN_TELEGRAM:
            anuncio["motivo"] = "score_insuficiente"
        elif roi_data["roi"] < ROI_POTENCIAL_MIN:
            anuncio["motivo"] = "roi_bajo"
        else:
            anuncio["motivo"] = "descartado_otros_criterios" # Motivo general si no cae en los anteriores

        return anuncio

    except Exception as e:
        logger.error(f"Error analizando enlace '{link}': {e}", exc_info=True) # exc_info para traceback completo
        # Intentar tomar una captura de pantalla en caso de error para depuración.
        if page:
            try:
                await page.screenshot(path=f"error_screenshot_{os.path.basename(link).replace('.', '_')}.png")
                logger.debug(f"Captura de pantalla guardada para {link}")
            except Exception as ss_e:
                logger.warning(f"No se pudo tomar captura de pantalla para {link}: {ss_e}")
        return None
    finally:
        # Asegurarse de cerrar la página después de su uso.
        if page:
            await page.close()

async def main_scraper():
    """
    Función principal del scraper que orquesta la extracción, análisis y almacenamiento
    de anuncios de vehículos de Facebook Marketplace.
    """
    # Recuperar configuraciones desde variables de entorno.
    modelo_objetivo = os.environ.get("MODELO_OBJETIVO", "civic")
    max_scrolls = int(os.environ.get("MAX_SCROLLS", "12"))
    headless_mode = os.environ.get("HEADLESS", "true").lower() in ("1", "true", "yes")

    logger.info(f"🚀 Iniciando scraper para modelo: '{modelo_objetivo}'.")
    logger.info(f"Modo headless: {'Activado' if headless_mode else 'Desactivado'}.")

    # Inicializar la base de datos (crear tabla y columnas si es necesario).
    inicializar_tabla_anuncios()

    # Usar async with para asegurar que playwright se cierre correctamente.
    async with async_playwright() as p:
        browser: Optional[Browser] = None
        try:
            # --- Configuración inicial del navegador para extracción de enlaces ---
            browser = await p.chromium.launch(
                headless=headless_mode,
                timeout=60000 # Timeout para el lanzamiento del navegador.
            )
            context_links = await cargar_contexto_con_cookies(browser)
            page_links = await context_links.new_page()

            # Navegar a la URL de búsqueda.
            url_busqueda = f"https://www.facebook.com/marketplace/search/?query={modelo_objetivo}"
            await page_links.goto(url_busqueda, timeout=60000) # Timeout para la navegación inicial.
            logger.info(f"🔍 Buscando en: {url_busqueda}")

            # Extraer enlaces realizando scrolls.
            enlaces_encontrados = await extraer_enlaces(page_links, modelo_objetivo, max_scrolls=max_scrolls)
            logger.info(f"✅ Encontrados {len(enlaces_encontrados)} enlaces únicos para '{modelo_objetivo}'.")

            # Cierre de la página y contexto usados para extracción de enlaces.
            await page_links.close()
            await context_links.close()
            # El navegador se cierra al final del 'async with browser' bloque.

            # --- Procesamiento de cada enlace ---
            # Reutilizar el mismo navegador para el análisis de enlaces individuales,
            # pero usando un nuevo contexto o páginas para cada análisis si se desea aislamiento.
            # Para eficiencia, se puede usar un solo contexto y múltiples páginas dentro de él,
            # controlando la concurrencia.
            
            # Reutiliza el navegador principal, no relanza uno nuevo.
            context_anuncios = await cargar_contexto_con_cookies(browser)

            # Métricas para el resumen final.
            metricas_sesion = {
                "total_procesados": 0,
                "validos_guardados": 0,
                "descartados": 0,
                "errores_analisis": 0
            }

            # Procesar enlaces con concurrencia limitada
            # Se usa asyncio.Semaphore para limitar la cantidad de páginas/tareas simultáneas.
            MAX_CONCURRENT_PAGES = 5 # Ajustar según los recursos disponibles y la tolerancia del sitio.
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

            async def process_single_link(link: str):
                async with semaphore: # Adquirir un "permiso" del semáforo.
                    metricas_sesion["total_procesados"] += 1
                    try:
                        anuncio = await analizar_enlace(context_anuncios, link, modelo_objetivo)
                        if anuncio:
                            # Insertar el anuncio en la base de datos.
                            insertar_anuncio_en_db(anuncio)
                            
                            # Actualizar contadores según el motivo final.
                            if anuncio.get("motivo") == "candidato_valido":
                                metricas_sesion["validos_guardados"] += 1
                            else:
                                metricas_sesion["descartados"] += 1
                                
                            logger.info(
                                f"Anuncio procesado ({metricas_sesion['total_procesados']}/{len(enlaces_encontrados)}) - '{link}' → Motivo: {anuncio['motivo']} | "
                                f"Precio: Q{anuncio['precio']:,} | "
                                f"ROI: {anuncio['roi']:.1f}% | "
                                f"Score: {anuncio['score']}/10"
                            )
                        else:
                            metricas_sesion["descartados"] += 1 # Un anuncio None significa que no se pudo analizar.
                            logger.warning(f"Anuncio '{link}' no pudo ser analizado completamente (retornó None).")

                    except Exception as e:
                        metricas_sesion["errores_analisis"] += 1
                        logger.error(f"Error crítico al procesar '{link}': {e}")
            
            # Crear y ejecutar todas las tareas de análisis concurrentemente.
            tasks = [process_single_link(link) for link in enlaces_encontrados]
            await asyncio.gather(*tasks) # Esperar a que todas las tareas concurrentes finalicen.

            logger.info(
                f"📊 Resumen final - Total enlaces procesados: {metricas_sesion['total_procesados']} | "
                f"Anuncios válidos guardados: {metricas_sesion['validos_guardados']} | "
                f"Anuncios descartados: {metricas_sesion['descartados']} | "
                f"Errores en análisis: {metricas_sesion['errores_analisis']}"
            )

        except Exception as e:
            logger.critical(f"❌ Error crítico en el scraper principal: {e}", exc_info=True)
            # Re-lanzar la excepción para que el llamador sepa que el scraper falló.
            raise
        finally:
            # Asegurarse de cerrar el navegador al finalizar, incluso si hay errores.
            if browser:
                await browser.close()
            logger.info("Scraper finalizado.")

if __name__ == "__main__":
    # Ejecutar la función principal asíncrona.
    asyncio.run(main_scraper())
