import asyncio
import os
import sqlite3
import logging
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError # Import ZoneInfoNotFoundError for robust error handling
from telegram import Bot
from telegram.error import TelegramError # Import specific TelegramError for better handling
from telegram.helpers import escape_markdown # Potentially useful, but escapar_multilinea is custom

# Importaciones desde scraper_marketplace (asumiendo que está en el mismo nivel o ruta accesible)
from scraper_marketplace import main_scraper as buscar_autos_marketplace

# Importaciones desde utils_analisis
from utils_analisis import (
    inicializar_tabla_anuncios,
    analizar_mensaje,
    limpiar_link,
    es_extranjero,
    modelos_bajo_rendimiento,
    MODELOS_INTERES,
    escapar_multilinea, # Custom function for MarkdownV2 escaping
    validar_coherencia_precio_año,
    Config
)

# --- Configuración de logging ---
logging.basicConfig(
    level=logging.INFO, # Nivel de logging general
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__) # Obtener logger para este módulo

# --- Inicialización de componentes ---
# Inicializar la tabla de anuncios en la base de datos al inicio del script.
inicializar_tabla_anuncios()

# Configuración del bot de Telegram
try:
    BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
    CHAT_ID = int(os.environ["CHAT_ID"].strip())
except KeyError as e:
    logger.critical(f"Error: Variable de entorno no encontrada: {e}. Asegúrate de definir BOT_TOKEN y CHAT_ID.")
    exit(1) # Salir si las variables esenciales no están configuradas.
except ValueError as e:
    logger.critical(f"Error: CHAT_ID debe ser un número entero: {e}.")
    exit(1)

DB_PATH = Config.DB_PATH  # Usar la misma ruta que en utils_analisis para consistencia
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) # Crear el directorio de la DB si no existe

bot = Bot(token=BOT_TOKEN) # Instancia del bot de Telegram

# --- Funciones de soporte para Telegram ---

async def safe_send(text: str, parse_mode: str = "MarkdownV2"):
    """
    Envía un mensaje a Telegram con reintentos y manejo de errores.
    Aplica escape_markdown V2 a todo el texto por defecto.

    Args:
        text (str): El texto del mensaje a enviar.
        parse_mode (str): El modo de parseo del mensaje (ej., "MarkdownV2", "HTML").
    """
    escaped_text = escapar_multilinea(text) # Asegurar que el texto esté correctamente escapado

    for attempt in range(3): # Intentar enviar el mensaje hasta 3 veces
        try:
            return await bot.send_message(
                chat_id=CHAT_ID,
                text=escaped_text,
                parse_mode=parse_mode,
                disable_web_page_preview=True # Deshabilitar previsualizaciones de enlaces para mensajes limpios
            )
        except TelegramError as e:
            logger.warning(f"Error de Telegram al enviar mensaje (intento {attempt + 1}/3): {e}")
            if "Too Many Requests" in str(e): # Manejo específico de RateLimitError
                retry_after = 5 # Default retry time
                try:
                    # Intenta extraer el tiempo de reintento si está en el error
                    match = re.search(r'retry after (\d+)', str(e))
                    if match:
                        retry_after = int(match.group(1))
                except Exception:
                    pass # Fallback to default if parsing fails
                logger.info(f"Rate limit excedido. Reintentando en {retry_after} segundos...")
                await asyncio.sleep(retry_after)
            else:
                await asyncio.sleep(1) # Esperar un poco antes de reintentar por otros errores
        except Exception as e:
            logger.error(f"Error inesperado al enviar mensaje a Telegram (intento {attempt + 1}/3): {e}", exc_info=True)
            await asyncio.sleep(1)
    logger.error(f"Fallo al enviar mensaje a Telegram después de múltiples reintentos: '{text[:100]}...'")

def dividir_y_enviar(titulo: str, items: List[str]) -> List[str]:
    """
    Divide una lista de ítems en bloques de texto más pequeños para cumplir con
    el límite de caracteres de Telegram (4096, pero usamos 3000 como margen).

    Args:
        titulo (str): Título que precederá al contenido de cada bloque.
        items (list[str]): Lista de cadenas, donde cada cadena es un elemento a incluir.

    Returns:
        List[str]: Una lista de cadenas, cada una representando un bloque de mensaje listo para enviar.
    """
    if not items:
        return []

    bloques = []
    current_block = [titulo]
    current_length = len(titulo) + 2 # Longitud del título + salto de línea doble

    for item in items:
        # Longitud del ítem actual + saltos de línea adicionales
        item_length = len(item) + 2

        if current_length + item_length > 3000: # Si añadir el ítem excede el límite
            bloques.append("\n\n".join(current_block)) # Añadir el bloque actual
            current_block = [titulo] # Iniciar un nuevo bloque con el título
            current_length = len(titulo) + 2 # Resetear la longitud del bloque

        current_block.append(item)
        current_length += item_length

    if current_block: # Añadir el último bloque si no está vacío
        bloques.append("\n\n".join(current_block))

    return bloques

# --- Función principal de envío de ofertas ---

async def enviar_ofertas():
    """
    Función principal que coordina la búsqueda de anuncios, su análisis y el envío
    de notificaciones a través de Telegram.
    """
    logger.info("📡 Iniciando bot de Telegram para enviar ofertas.")

    try:
        # Obtener la hora actual en la zona horaria de Guatemala
        guatemala_tz = ZoneInfo("America/Guatemala")
        now_local = datetime.now(guatemala_tz)
    except ZoneInfoNotFoundError:
        logger.error("La zona horaria 'America/Guatemala' no se encontró. Usando UTC.")
        now_local = datetime.now(ZoneInfo("UTC"))
    except Exception as e:
        logger.error(f"Error al obtener la hora local: {e}. Usando UTC.")
        now_local = datetime.now(ZoneInfo("UTC"))


    # Identificar modelos de bajo rendimiento para no considerarlos en la búsqueda activa (si aplica)
    bajos_rendimiento = modelos_bajo_rendimiento()
    # Se asume que MODELOS_INTERES es una lista global de modelos a buscar.
    modelos_activos = [m for m in MODELOS_INTERES if m not in bajos_rendimiento]
    logger.info(f"✅ Modelos activos para búsqueda: {modelos_activos}")

    try:
        # Ejecutar el scraper principal para buscar autos en Marketplace
        # `buscar_autos_marketplace` devuelve (enlaces_brutos, enlaces_pendientes, metricas_scraper)
        # Solo necesitamos los `enlaces_brutos` (texto_crudo de los anuncios) para el análisis.
        # Las métricas del scraper se imprimen en el propio scraper.
        brutos, _, _ = await buscar_autos_marketplace()
    except Exception as e:
        logger.error(f"❌ Error al ejecutar el scraper de Marketplace: {e}", exc_info=True)
        await safe_send("❌ Error ejecutando el scraper de Marketplace, revisa los logs del servidor.")
        return # Salir si el scraper falla críticamente.

    # Listas para almacenar anuncios categorizados
    buenos_candidatos: List[str] = [] # Para anuncios relevantes que se envían a Telegram
    potenciales_revision: List[str] = [] # Para anuncios que cumplen criterios mínimos pero no "relevantes"
    
    # Listas para resúmenes de logging
    resumen_relevantes: List[Tuple[str, str, float, int]] = [] # (modelo, url, roi, score)
    resumen_potenciales: List[Tuple[str, str, float, int]] = []

    # Diccionario para contar los motivos de descarte
    motivos_descarte: Dict[str, int] = {
        "incompleto": 0,
        "extranjero": 0,
        "modelo no detectado": 0, # Este se usaría si `coincide_modelo` falla
        "anio_fuera_de_rango": 0,
        "precio_fuera_de_rango": 0,
        "precio_anio_incoherente": 0,
        "roi_bajo": 0,
        "score_insuficiente": 0,
        "contiene_palabras_negativas": 0,
        "desconocido": 0 # Para cualquier otro caso no categorizado explícitamente.
    }

    total_enlaces_procesados = len(brutos)
    logger.info(f"Iniciando análisis de {total_enlaces_procesados} anuncios brutos.")

    # Analizar cada anuncio bruto
    for i, txt_crudo in enumerate(brutos):
        logger.info(f"Procesando anuncio {i+1}/{total_enlaces_procesados}...")
        
        # `analizar_mensaje` procesa el texto crudo y aplica la lógica de negocio
        res = analizar_mensaje(txt_crudo)
        
        if not res:
            motivos_descarte["incompleto"] += 1
            logger.debug(f"Anuncio descartado: datos incompletos. Texto inicial: '{txt_crudo[:100]}...'")
            continue

        # Extraer datos del resultado del análisis
        url = res.get("link", "N/A") # Usar .get para evitar KeyError si la clave no existe
        modelo = res.get("modelo", "Desconocido")
        anio = res.get("anio", 0)
        precio = res.get("precio", 0.0)
        roi = res.get("roi", 0.0)
        score = res.get("score", 0)
        relevante = res.get("relevante", 0) # 0 o 1
        motivo_analisis = res.get("motivo", "desconocido") # Motivo ya categorizado por `analizar_enlace`

        logger.info(f"Anuncio: {modelo} | Año: {anio} | Precio: Q{precio:,} | ROI: {roi:.1f}% | Score: {score}/10 | Relevante: {bool(relevante)}")

        # Construir el mensaje formateado para Telegram
        mensaje_telegram = (
            f"🚘 *{modelo.title()}*\n"
            f"• Año: {anio if anio != 0 else 'N/A'}\n"
            f"• Precio: Q{precio:,}\n"
            f"• ROI: {roi:.1f}%\n"
            f"• Score: {score}/10\n"
            f"🔗 {url}"
        )

        # Usar el motivo_analisis directamente de `analizar_enlace` para el conteo de descartes
        if not relevante:
            motivos_descarte[motivo_analisis] = motivos_descarte.get(motivo_analisis, 0) + 1
            logger.debug(f"Anuncio descartado: {motivo_analisis} -> {url}")
        else:
            buenos_candidatos.append(mensaje_telegram)
            resumen_relevantes.append((modelo, url, roi, score))
            logger.info(f"Anuncio relevante encontrado: {url}")

        # Agregar a potenciales si cumple criterios mínimos específicos para "potenciales"
        # Se asume que Config.SCORE_MIN_DB y Config.ROI_MINIMO definen este umbral.
        if (not relevante and score >= Config.SCORE_MIN_DB and roi >= Config.ROI_MINIMO):
            potenciales_revision.append(mensaje_telegram)
            resumen_potenciales.append((modelo, url, roi, score))
            logger.info(f"Anuncio potencial encontrado: {url}")

    # --- Resumen y envío de mensajes a Telegram ---
    total_relevantes = len(buenos_candidatos)
    total_potenciales = len(potenciales_revision)
    total_descartados = sum(motivos_descarte.values()) - motivos_descarte.get("desconocido", 0) # Excluir 'desconocido' si no se asignó explícitamente

    await safe_send(f"📊 *Resumen de Ejecución a las {now_local.strftime('%H:%M')} (GMT-6):*\n"
                    f"• Total anuncios procesados: {total_enlaces_procesados}\n"
                    f"• Anuncios relevantes: {total_relevantes}\n"
                    f"• Anuncios potenciales: {total_potenciales}\n"
                    f"• Anuncios descartados: {total_descartados}")

    # Enviar resumen de descartes detallado si hay alguno
    if total_descartados > 0:
        detalles_descarte = "\n".join(f"• {k.replace('_', ' ').title()}: {v}"
                                      for k, v in motivos_descarte.items() if v > 0)
        await safe_send(f"📉 *Detalle de descartes:*\n{detalles_descarte}")

    # Si no hay ofertas relevantes ni potenciales, enviar un mensaje informativo
    if not buenos_candidatos and not potenciales_revision:
        if now_local.hour in [6, 12, 18, 0]: # Mensaje más conciso para ejecuciones horarias
            await safe_send(f"💤 Ejecución a las {now_local.strftime('%H:%M')}, sin ofertas nuevas en los rangos de interés.")
        else: # Mensaje general si no hay nada
             await safe_send("🤷‍♂️ No se encontraron anuncios relevantes o potenciales en esta ejecución.")
        return # Terminar la función si no hay nada que enviar

    # Enviar ofertas destacadas (relevantes)
    if buenos_candidatos:
        for bloque in dividir_y_enviar("✨ *Nuevas Ofertas Destacadas:*\n", buenos_candidatos):
            await safe_send(bloque)

    # Enviar ofertas potenciales (que cumplen umbrales mínimos pero no son "relevantes")
    if potenciales_revision:
        for bloque in dividir_y_enviar("👀 *Otras Oportunidades (Revisión Manual):*\n", potenciales_revision):
            await safe_send(bloque)

    # --- Reporte final y logging detallado ---
    # Reportar el total de anuncios acumulados en la base de datos
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM anuncios")
            total_db_acumulado = cur.fetchone()[0]
            await safe_send(f"💾 Total de anuncios acumulados en la base de datos: {total_db_acumulado}")
    except sqlite3.Error as e:
        logger.error(f"Error al consultar la base de datos para el total acumulado: {e}", exc_info=True)
        await safe_send("⚠️ Error al consultar el total de anuncios en la base de datos.")


    logger.info("\n--- Resumen Detallado de Anuncios (para logs) ---")
    logger.info(f"Total de anuncios procesados: {total_enlaces_procesados}")
    logger.info(f"Anuncios relevantes enviados a Telegram: {total_relevantes}")
    logger.info(f"Anuncios potenciales enviados a Telegram: {total_potenciales}")
    logger.info(f"Total de anuncios descartados: {total_descartados}")

    if resumen_relevantes:
        logger.info("\n--- Anuncios Relevantes ---")
        for modelo, url, roi, score in resumen_relevantes:
            logger.info(f"• Modelo: {modelo.title()} | ROI: {roi:.1f}% | Score: {score}/10 | Link: {url}")

    if resumen_potenciales:
        logger.info("\n--- Anuncios Potenciales ---")
        for modelo, url, roi, score in resumen_potenciales:
            logger.info(f"• Modelo: {modelo.title()} | ROI: {roi:.1f}% | Score: {score}/10 | Link: {url}")

    logger.info("\n--- Motivos de Descarte ---")
    for motivo, count in motivos_descarte.items():
        if count > 0:
            logger.info(f"• {motivo.replace('_', ' ').title()}: {count} anuncios")

    logger.info("\n--- Fin de la ejecución del bot ---")

if __name__ == "__main__":
    # Ejecutar la función principal asíncrona del bot
    asyncio.run(enviar_ofertas())
