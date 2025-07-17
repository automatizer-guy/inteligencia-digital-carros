import os
import re
import json
import random
import asyncio
import logging
from urllib.parse import urlparse
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
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

# Configuración de logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Sincronizar configuración
DB_PATH = os.path.abspath(os.environ.get("DB_PATH", "upload-artifact/anuncios.db"))
Config.DB_PATH = DB_PATH

MIN_PRECIO_VALIDO = 3000
ROI_POTENCIAL_MIN = Config.ROI_MINIMO - 10

def limpiar_url(link: str) -> str:
    """Normalizar URLs de Facebook Marketplace"""
    if not link:
        return ""
    path = urlparse(link.strip()).path.rstrip("/")
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser: Browser) -> BrowserContext:
    """Configurar contexto del navegador con cookies"""
    context = await browser.new_context()
    cookies_path = "cookies_marketplace.json"
    if os.path.exists(cookies_path):
        try:
            with open(cookies_path, "r") as f:
                cookies = json.load(f)
                await context.add_cookies(cookies)
                logger.info("Cookies cargadas correctamente")
        except Exception as e:
            logger.warning(f"Error cargando cookies: {e}")
    return context

async def extraer_enlaces(page: Page, modelo: str, max_scrolls: int = 12) -> List[str]:
    """Extraer enlaces de anuncios con scroll infinito"""
    enlaces = set()
    contador_scroll = 0
    
    logger.info(f"Iniciando extracción de enlaces para {modelo}")
    
    while contador_scroll < max_scrolls:
        try:
            # Scroll y espera aleatoria
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await asyncio.sleep(random.uniform(2, 4))
            
            # Extraer nuevos enlaces
            nuevos = await page.query_selector_all("a[href*='/marketplace/item/']")
            for elemento in nuevos:
                href = await elemento.get_attribute("href")
                if href:
                    enlaces.add(limpiar_url(href))
            
            contador_scroll += 1
            logger.info(f"Scroll {contador_scroll}/{max_scrolls} - Enlaces encontrados: {len(enlaces)}")
            
        except Exception as e:
            logger.error(f"Error durante scroll: {e}")
            break
    
    return list(enlaces)

async def extraer_datos_del_anuncio(page: Page) -> Dict[str, str]:
    """Extraer datos estructurados de un anuncio"""
    try:
        await asyncio.sleep(random.uniform(1, 3))
        
        # Extraer el texto principal
        elemento_contenido = await page.query_selector("div[aria-label='Contenido de la página']")
        if not elemento_contenido:
            return {}
        
        texto_total = await elemento_contenido.inner_text()
        
        # Patrones mejorados para extracción
        patrones = {
            "precio_texto": r"(Q[\d\.,]+|\$[\d\.,]+)",
            "titulo": r"^(.*?)(?:\n|$)",
            "descripcion": r"Descripción\n(.*?)(?:\n[A-Z]|$)",
            "ubicacion": r"Ubicación\n(.*?)(?:\n|$)",
            "kilometraje": r"Kilometraje\n(.*?)(?:\n|$)"
        }

        datos = {}
        for campo, patron in patrones.items():
            match = re.search(patron, texto_total, re.MULTILINE | re.IGNORECASE | re.DOTALL)
            datos[campo] = match.group(1).strip() if match else ""

        datos["texto_crudo"] = texto_total
        return datos
        
    except Exception as e:
        logger.error(f"Error extrayendo datos: {e}")
        return {}

async def analizar_enlace(context: BrowserContext, link: str, modelo: str) -> Optional[Dict[str, any]]:
    """Analizar un enlace individual y extraer datos estructurados"""
    page = await context.new_page()
    try:
        # Navegar y extraer datos
        await page.goto(link, timeout=60000)
        datos = await extraer_datos_del_anuncio(page)
        
        if not datos:
            return None

        # Procesar datos básicos
        precio = limpiar_precio(datos.get("precio_texto", ""))
        texto = f"{datos.get('titulo', '')} {datos.get('descripcion', '')}"
        anio = extraer_anio(texto)
        km = datos.get("kilometraje", "")

        # Estructura base del anuncio
        anuncio = {
            "link": limpiar_link(link),
            "precio": precio,
            "modelo": modelo,
            "anio": anio,
            "km": km,
            "descripcion": datos.get("descripcion", ""),
            "texto_crudo": datos.get("texto_crudo", ""),
            "ubicacion": datos.get("ubicacion", ""),
            "score": 0,
            "roi": 0,
            "motivo": "",
            "confianza_precio": "baja",
            "muestra_precio": 0,
            "relevante": 0
        }

        # Validación inicial
        valido, motivo = validar_anuncio_completo(
            texto=anuncio["texto_crudo"],
            precio=precio,
            anio=anio,
            modelo=modelo
        )
        if not valido:
            anuncio["motivo"] = motivo
            return anuncio

        # Filtros adicionales
        if contiene_negativos(texto.lower())):
            anuncio["motivo"] = "contiene palabras negativas"
            return anuncio

        if not coincide_modelo(texto.lower(), modelo):
            anuncio["motivo"] = "no coincide modelo"
            return anuncio

        # Cálculo de ROI y puntuación
        roi_data = calcular_roi_real(modelo, precio, anio)
        score = puntuar_anuncio(anuncio["texto_crudo"], roi_data)

        # Actualizar anuncio con datos calculados
        anuncio.update({
            "score": score,
            "roi": roi_data["roi"],
            "confianza_precio": roi_data["confianza"],
            "muestra_precio": roi_data["muestra"],
            "relevante": 1 if score >= Config.SCORE_MIN_TELEGRAM and roi_data["roi"] >= Config.ROI_MINIMO else 0
        })

        # Determinar motivo final
        if score < Config.SCORE_MIN_TELEGRAM:
            anuncio["motivo"] = "score insuficiente"
        elif roi_data["roi"] < ROI_POTENCIAL_MIN:
            anuncio["motivo"] = "ROI bajo"
        else:
            anuncio["motivo"] = "candidato válido"

        return anuncio

    except Exception as e:
        logger.error(f"Error analizando {link}: {e}")
        return None
    finally:
        await page.close()

async def main_scraper():
    """Función principal del scraper"""
    modelo = os.environ.get("MODELO_OBJETIVO", "civic")
    max_scrolls = int(os.environ.get("MAX_SCROLLS", "12"))
    headless_mode = os.environ.get("HEADLESS", "true").lower() in ("1", "true")

    logger.info(f"🚀 Iniciando scraper para modelo: {modelo}")
    logger.info(f"Modo headless: {'Activado' if headless_mode else 'Desactivado'}")

    # Inicializar base de datos
    inicializar_tabla_anuncios()

    async with async_playwright() as p:
        try:
            # Configuración inicial del navegador
            browser = await p.chromium.launch(
                headless=headless_mode,
                timeout=60000
            )
            context = await cargar_contexto_con_cookies(browser)
            page = await context.new_page()

            # Búsqueda inicial
            url_busqueda = f"https://www.facebook.com/marketplace/search/?query={modelo}"
            await page.goto(url_busqueda, timeout=60000)
            logger.info(f"🔍 Buscando en: {url_busqueda}")

            # Extraer enlaces
            enlaces = await extraer_enlaces(page, modelo, max_scrolls=max_scrolls)
            logger.info(f"✅ Encontrados {len(enlaces)} enlaces únicos")

            # Cerrar navegador inicial
            await browser.close()

            # Procesar cada enlace
            browser = await p.chromium.launch(headless=headless_mode)
            context = await cargar_contexto_con_cookies(browser)

            metricas = {
                "total": 0,
                "validos": 0,
                "descartados": 0,
                "errores": 0
            }

            for link in enlaces:
                metricas["total"] += 1
                try:
                    anuncio = await analizar_enlace(context, link, modelo)
                    if anuncio:
                        insertar_anuncio_en_db(anuncio)
                        
                        if anuncio["motivo"] == "candidato válido":
                            metricas["validos"] += 1
                        else:
                            metricas["descartados"] += 1
                            
                        logger.info(
                            f"{link} → {anuncio['motivo']} | "
                            f"Precio: Q{anuncio['precio']:,} | "
                            f"ROI: {anuncio['roi']:.1f}% | "
                            f"Score: {anuncio['score']}/10"
                        )
                except Exception as e:
                    metricas["errores"] += 1
                    logger.error(f"Error procesando {link}: {e}")

            logger.info(f"📊 Resumen final - Total: {metricas['total']} | "
                        f"Válidos: {metricas['validos']} | "
                        f"Descartados: {metricas['descartados']} | "
                        f"Errores: {metricas['errores']}")

            return enlaces, [], metricas

        except Exception as e:
            logger.error(f"❌ Error crítico en el scraper: {e}")
            raise
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main_scraper())
