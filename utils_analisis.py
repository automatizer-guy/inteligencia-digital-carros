import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager
from functools import lru_cache
from collections import defaultdict
import logging

# --- Configuración centralizada ---
class Config:
    """
    Clase para gestionar la configuración centralizada de la aplicación.
    Permite un fácil acceso y modificación de parámetros globales.
    """
    # Configuración de la base de datos
    DB_PATH: str = os.path.abspath(os.environ.get("DB_PATH", "upload-artifact/anuncios.db"))
    
    # Parámetros de scoring y filtros
    SCORE_MIN_DB: int = 4
    SCORE_MIN_TELEGRAM: int = 6
    ROI_MINIMO: float = 10.0
    TOLERANCIA_PRECIO_REF: int = 1
    DEPRECIACION_ANUAL: float = 0.08
    MUESTRA_MINIMA_CONFIABLE: int = 5
    MUESTRA_MINIMA_MEDIA: int = 2
    
    # Parámetros de validación
    PRECIO_MIN_VALIDO: int = 5000
    PRECIO_MAX_VALIDO: int = 500000
    AÑO_MIN_VALIDO: int = 1990
    
    # Modo de depuración
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")

# --- Configuración de logging ---
# Se configura el nivel de logging basado en el modo DEBUG de la clase Config.
# El formato incluye timestamp, nivel, nombre del logger y mensaje.
logging.basicConfig(
    level=logging.INFO if Config.DEBUG else logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# --- Constantes ---
AÑO_ACTUAL: int = datetime.now().year

# Precios de referencia por defecto para modelos comunes.
# Usado cuando no hay suficientes datos en la base de datos.
PRECIOS_POR_DEFECTO: Dict[str, int] = {
    "yaris": 45000, "civic": 65000, "corolla": 50000, "sentra": 42000,
    "rav4": 130000, "cr-v": 95000, "tucson": 70000, "kia picanto": 35000,
    "chevrolet spark": 30000, "nissan march": 37000, "suzuki alto": 26000,
    "suzuki swift": 40000, "hyundai accent": 41000, "mitsubishi mirage": 33000,
    "suzuki grand vitara": 52000, "hyundai i10": 34000, "kia rio": 40000,
    "toyota": 48000, "honda": 50000
}
MODELOS_INTERES: List[str] = list(PRECIOS_POR_DEFECTO.keys())

# Palabras clave que indican que un anuncio podría no ser de interés (ej. venta de repuestos).
PALABRAS_NEGATIVAS: List[str] = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "motor fundido", "no arranca", "no enciende", "papeles atrasados",
    "sin motor", "para partes", "no funciona", "accidentado"
]

# Lugares que, si se mencionan en el anuncio, sugieren que no es local.
LUGARES_EXTRANJEROS: List[str] = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Sinónimos para modelos de vehículos para mejorar la coincidencia en el texto.
SINONIMOS_MODELO: Dict[str, List[str]] = {
    "accent": ["acent", "acsent", "accent rb", "hyundai rb", "rb15", "hyundai acent", "accen"],
    "civic": ["civc", "civic lx", "civic ex", "civic sport", "cvic", "civic 1.8", "honda civic"],
    "sentra": ["sentran", "sentra b13", "nissan b13", "nissan sentra", "sentr4", "sentra clásico"],
    "rio": ["rio5", "kia rio", "rio lx", "rio x", "rio x-line", "kia hatchback", "kia ryo"],
    "swift": ["swift sport", "swift gl", "suzuki swift", "swift dzire", "swft", "swift 1.2"],
    "march": ["nissan march", "march active", "march sense", "m4rch"],
    "yaris": ["toyota yaris", "yaris hb", "yariz", "yaris core", "yaris s"],
    "cr-v": ["crv", "cr-v lx", "honda cr-v", "cr b", "crv ex", "crv turbo"],
    "tucson": ["hyundai tucson", "tucsón", "tuczon", "tucson gls", "tucson ix"],
    "spark": ["chevrolet spark", "spark gt", "sp4rk", "spark life"],
    "picanto": ["kia picanto", "picanto xline", "pikanto", "picanto 1.2"],
    "alto": ["suzuki alto", "alto 800", "alt0", "alto std"],
    "grand vitara": ["suzuki grand vitara", "gran vitara", "vitara 4x4", "grandvitara"]
}

# --- Métricas de sesión ---
class MetricasSession:
    """
    Clase para recolectar métricas de ejecución de la sesión.
    Permite rastrear contadores, errores y advertencias.
    """
    def __init__(self):
        """Inicializa la sesión de métricas con la hora de inicio y contadores vacíos."""
        self.inicio: datetime = datetime.now()
        self.contadores: Dict[str, int] = defaultdict(int)
        self.errores: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
    
    def incrementar(self, metrica: str, cantidad: int = 1):
        """
        Incrementa un contador de métrica específico.

        Args:
            metrica (str): El nombre de la métrica a incrementar.
            cantidad (int): La cantidad a incrementar (por defecto es 1).
        """
        self.contadores[metrica] += cantidad
    
    def error(self, mensaje: str, excepcion: Optional[Exception] = None):
        """
        Registra un error en las métricas y en el logger.

        Args:
            mensaje (str): Mensaje descriptivo del error.
            excepcion (Optional[Exception]): Objeto de excepción asociado, si existe.
        """
        error_info = {
            "timestamp": datetime.now(),
            "mensaje": mensaje,
            "excepcion": str(excepcion) if excepcion else None
        }
        self.errores.append(error_info)
        logger.error(f"{mensaje}: {excepcion}" if excepcion else mensaje)
    
    def warning(self, mensaje: str):
        """
        Registra una advertencia en las métricas y en el logger.

        Args:
            mensaje (str): Mensaje descriptivo de la advertencia.
        """
        warning_info = {
            "timestamp": datetime.now(),
            "mensaje": mensaje
        }
        self.warnings.append(warning_info)
        logger.warning(mensaje)
    
    def resumen(self) -> Dict[str, Any]:
        """
        Genera un resumen de las métricas de la sesión.

        Returns:
            Dict[str, Any]: Un diccionario con el resumen de las métricas.
        """
        duracion = datetime.now() - self.inicio
        return {
            "duracion_segundos": duracion.total_seconds(),
            "contadores": dict(self.contadores),
            "total_errores": len(self.errores),
            "total_warnings": len(self.warnings),
            "errores": self.errores[-5:],  # Solo últimos 5 errores para evitar sobrecarga
            "warnings": self.warnings[-5:]  # Solo últimos 5 warnings
        }

# Instancia global de métricas, usada para registrar eventos en toda la aplicación.
metricas = MetricasSession()

# --- Funciones de utilidad general ---

def escapar_multilinea(texto: str) -> str:
    """
    Escapa caracteres especiales en un texto para ser compatible con Telegram MarkdownV2.

    Args:
        texto (str): El texto a escapar.

    Returns:
        str: El texto con los caracteres especiales escapados.
    """
    # Los caracteres `\-` deben ir al final del set `[]` para ser interpretados literalmente.
    return re.sub(r'([_*\[\]()~>#+=|{}.!-])', r'\\\1', texto)

# Inicialización del directorio de la base de datos si no existe.
os.makedirs(os.path.dirname(Config.DB_PATH), exist_ok=True)

def timeit(func):
    """
    Decorador para medir el tiempo de ejecución de una función.
    Solo se activa si Config.DEBUG es True.
    """
    def wrapper(*args, **kwargs):
        if not Config.DEBUG:
            return func(*args, **kwargs)
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.debug(f"⌛ {func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

@contextmanager
def get_db_connection():
    """
    Context manager para gestionar conexiones de base de datos SQLite3.
    Asegura que la conexión se cierre correctamente y maneja las transacciones.
    """
    conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
    conn.isolation_level = None # Habilita el autocommit (o rollback manual)
    try:
        yield conn
        conn.commit() # Realiza commit si no hubo excepciones
    except Exception as e:
        conn.rollback() # Realiza rollback en caso de excepción
        metricas.error(f"Error en transacción de BD", e)
        raise # Re-lanza la excepción para que el llamador pueda manejarla
    finally:
        conn.close()

# Pool de conexiones simple (legado, se prefiere get_db_connection)
_conn: Optional[sqlite3.Connection] = None

def get_conn() -> sqlite3.Connection:
    """
    Obtener una conexión reutilizable a la base de datos (función legada).
    No se recomienda para nuevas funcionalidades, usar 'get_db_connection'.
    """
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
    return _conn

@timeit
def inicializar_tabla_anuncios():
    """
    Inicializa la tabla 'anuncios' en la base de datos, creándola si no existe
    o agregando columnas nuevas si son necesarias.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()

            # Verificar si la tabla existe
            cur.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='anuncios'
            """)
            
            if cur.fetchone() is None:
                # Crear tabla completa si no existe
                cur.execute("""
                    CREATE TABLE anuncios (
                        link TEXT PRIMARY KEY,
                        modelo TEXT,
                        anio INTEGER,
                        precio INTEGER,
                        km TEXT,
                        fecha_scrape DATE,
                        roi REAL,
                        score INTEGER,
                        updated_at TEXT DEFAULT CURRENT_DATE,
                        relevante INTEGER DEFAULT 0,
                        confianza_precio TEXT DEFAULT 'baja',
                        muestra_precio INTEGER DEFAULT 0
                    )
                """)
                logger.info("✅ Tabla 'anuncios' creada con estructura completa.")
            else:
                # Si la tabla existe, verificar y añadir columnas faltantes
                cur.execute("PRAGMA table_info(anuncios)")
                cols_existentes = {row[1] for row in cur.fetchall()}
                
                columnas_necesarias: List[Tuple[str, str]] = [
                    ("updated_at", "DATE DEFAULT DATE('now')"),
                    ("relevante", "INTEGER DEFAULT 0"),
                    ("confianza_precio", "TEXT DEFAULT 'baja'"),
                    ("muestra_precio", "INTEGER DEFAULT 0")
                ]
                
                for col_nombre, col_def in columnas_necesarias:
                    if col_nombre not in cols_existentes:
                        try:
                            cur.execute(f"ALTER TABLE anuncios ADD COLUMN {col_nombre} {col_def}")
                            logger.info(f"✅ Columna '{col_nombre}' agregada a la tabla 'anuncios'.")
                        except sqlite3.OperationalError as e:
                            # Esto puede ocurrir si la columna ya existe pero no se detectó por alguna razón,
                            # o si hay un bloqueo. Se registra como advertencia.
                            metricas.warning(f"No se pudo agregar la columna '{col_nombre}': {e}")

            # No es necesario conn.commit() aquí si se usa el context manager con isolation_level=None
            metricas.incrementar("tabla_inicializada")
            
    except Exception as e:
        metricas.error("Error inicializando tabla 'anuncios'.", e)
        raise

def validar_anuncio_completo(texto: str, precio: int, anio: int, modelo: str) -> Tuple[bool, str]:
    """
    Realiza una validación centralizada de un anuncio, devolviendo True/False
    y una razón de rechazo si no es válido.

    Args:
        texto (str): Texto completo del anuncio.
        precio (int): Precio del vehículo.
        anio (int): Año del vehículo.
        modelo (str): Modelo del vehículo.

    Returns:
        Tuple[bool, str]: Una tupla que contiene True si el anuncio es válido,
                          False en caso contrario, y una cadena con la razón del rechazo.
    """
    if not isinstance(precio, int) or not isinstance(anio, int) or not isinstance(modelo, str):
        # Añadir validación de tipos básicos para evitar errores inesperados más adelante
        return False, "tipo_de_dato_invalido"

    if precio < Config.PRECIO_MIN_VALIDO:
        return False, "precio_muy_bajo"
    if precio > Config.PRECIO_MAX_VALIDO:
        return False, "precio_muy_alto"
    if anio < Config.AÑO_MIN_VALIDO or anio > AÑO_ACTUAL:
        return False, "año_invalido"
    if not validar_coherencia_precio_año(precio, anio):
        return False, "precio_año_incoherente"
    if contiene_negativos(texto):
        return False, "contiene_negativos"
    if es_extranjero(texto):
        return False, "ubicacion_extranjera"
    return True, "valido"

def insertar_lote_anuncios(anuncios: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Inserta o actualiza un lote de anuncios en la base de datos dentro de una única transacción.

    Args:
        anuncios (List[Dict[str, Any]]): Una lista de diccionarios, cada uno representando un anuncio.

    Returns:
        Dict[str, int]: Un diccionario con el conteo de anuncios 'nuevos', 'actualizados' y 'errores'.
    """
    contadores = {"nuevos": 0, "actualizados": 0, "errores": 0}
    
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            for anuncio in anuncios:
                try:
                    cur.execute("""
                        INSERT INTO anuncios (
                          link, modelo, anio, precio, km, roi, score,
                          relevante, confianza_precio, muestra_precio,
                          fecha_scrape, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'), DATE('now'))
                        ON CONFLICT(link) DO UPDATE SET
                          modelo=excluded.modelo,
                          anio=excluded.anio,
                          precio=excluded.precio,
                          km=excluded.km,
                          roi=excluded.roi,
                          score=excluded.score,
                          relevante=excluded.relevante,
                          confianza_precio=excluded.confianza_precio,
                          muestra_precio=excluded.muestra_precio,
                          updated_at=DATE('now')
                    """, (
                        anuncio["link"], anuncio["modelo"], anuncio["anio"],
                        anuncio["precio"], anuncio["km"], anuncio["roi"],
                        anuncio["score"], int(anuncio["relevante"]), # Convertir booleano a int para DB
                        anuncio["confianza_precio"], anuncio["muestra_precio"]
                    ))
                    
                    # SQLite no siempre tiene lastrowid para UPDATES,
                    # pero en ON CONFLICT es una buena aproximación para determinar 'nuevo' vs 'actualizado'.
                    # Una forma más robusta sería una SELECT antes del INSERT/UPDATE, pero
                    # el DO UPDATE SET ya maneja la lógica de manera eficiente.
                    if cur.rowcount > 0 and cur.lastrowid is not None:
                        # Si lastrowid no es None, usualmente significa una inserción.
                        # Para ON CONFLICT, rowcount es más indicativo si hubo cambio.
                        contadores["nuevos"] += 1
                    else:
                        contadores["actualizados"] += 1
                        
                except sqlite3.Error as e:
                    contadores["errores"] += 1
                    # Se registra el error específico del anuncio, pero la transacción principal continuará.
                    metricas.error(f"Error insertando/actualizando anuncio: {anuncio.get('link', 'unknown')}", e)
            
            # El commit es manejado por el context manager get_db_connection
            metricas.incrementar("lote_insertado")
            
    except Exception as e:
        # Este error captura problemas a nivel de la transacción del lote o la conexión.
        metricas.error("Error general en inserción de lote de anuncios.", e)
        raise # Se re-lanza para manejo externo si es crítico
    
    return contadores

def insertar_o_actualizar_anuncio_db(
    conn: sqlite3.Connection,
    link: str,
    modelo: str,
    anio: int,
    precio: int,
    km: str,
    roi: float,
    score: int,
    relevante: bool,
    confianza_precio: str,
    muestra_precio: int
) -> str:
    """
    Inserta o actualiza un anuncio individual en la base de datos utilizando una conexión existente.
    Devuelve 'nuevo' si se insertó o 'actualizado' si ya existía y se modificó.

    Args:
        conn (sqlite3.Connection): La conexión a la base de datos.
        link (str): El enlace único del anuncio (clave primaria).
        modelo (str): Modelo del vehículo.
        anio (int): Año del vehículo.
        precio (int): Precio del vehículo.
        km (str): Kilometraje del vehículo.
        roi (float): Retorno de Inversión calculado.
        score (int): Puntuación del anuncio.
        relevante (bool): Indica si el anuncio es relevante.
        confianza_precio (str): Nivel de confianza del precio (ej. 'alta', 'baja').
        muestra_precio (int): Tamaño de la muestra usada para calcular la confianza del precio.

    Returns:
        str: "nuevo" si se insertó el anuncio, "actualizado" si se modificó.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO anuncios (
              link, modelo, anio, precio, km, roi, score,
              relevante, confianza_precio, muestra_precio,
              fecha_scrape, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'), DATE('now'))
            ON CONFLICT(link) DO UPDATE SET
              modelo=excluded.modelo,
              anio=excluded.anio,
              precio=excluded.precio,
              km=excluded.km,
              roi=excluded.roi,
              score=excluded.score,
              relevante=excluded.relevante,
              confianza_precio=excluded.confianza_precio,
              muestra_precio=excluded.muestra_precio,
              updated_at=DATE('now')
        """, (
            link, modelo, anio, precio, km, roi, score,
            int(relevante), confianza_precio, muestra_precio # Convertir booleano a int
        ))
        
        # El commit es manejado por el context manager get_db_connection si esta función es llamada
        # dentro de él. Si se llama directamente, se asume que la conexión se maneja externamente.
        
        # Determine if it was an insert or update. rowcount will be 1 for insert, 0 for update with no change, or 1 for update with change
        # A more robust check for 'new' vs 'updated' would involve a prior SELECT,
        # but for ON CONFLICT, the behavior around lastrowid and rowcount can vary.
        # This implementation aligns with the original intent.
        resultado = "nuevo" if cur.lastrowid else "actualizado"
        metricas.incrementar(f"anuncio_{resultado}")
        return resultado
        
    except sqlite3.Error as e:
        metricas.error(f"Error insertando/actualizando anuncio: {link}", e)
        raise

def insertar_anuncio_en_db(anuncio: Dict[str, Any]) -> str:
    """
    Inserta o actualiza un anuncio individual en la base de datos utilizando
    el context manager para la conexión.

    Args:
        anuncio (Dict[str, Any]): Diccionario con los datos del anuncio.

    Returns:
        str: "nuevo" si se insertó el anuncio, "actualizado" si se modificó.
    """
    with get_db_connection() as conn:
        return insertar_o_actualizar_anuncio_db(
            conn,
            anuncio['link'],
            anuncio['modelo'],
            anuncio['anio'],
            anuncio['precio'],
            anuncio.get('km', ''), # Usar .get() con valor por defecto para seguridad
            anuncio['roi'],
            anuncio['score'],
            anuncio.get('relevante', False), # Usar .get() con valor por defecto
            anuncio.get('confianza_precio', 'baja'), # Usar .get() con valor por defecto
            anuncio.get('muestra_precio', 0) # Usar .get() con valor por defecto
        )

def limpiar_anuncios_antiguos(dias: int = 30) -> int:
    """
    Elimina anuncios de la base de datos que son más antiguos que el número de días especificado.

    Args:
        dias (int): El número de días. Anuncios más antiguos que este valor serán eliminados.

    Returns:
        int: El número de anuncios eliminados.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM anuncios 
                WHERE fecha_scrape < date('now', ?)
            """, (f"-{dias} days",)) # Usar parámetros para seguridad contra SQL Injection
            eliminados = cur.rowcount
            # El commit es manejado por el context manager get_db_connection
            
            if eliminados > 0:
                logger.info(f"🗑️ Eliminados {eliminados} anuncios antiguos (>{dias} días).")
            
            metricas.incrementar("anuncios_eliminados", eliminados)
            return eliminados
            
    except Exception as e:
        metricas.error("Error limpiando anuncios antiguos.", e)
        return 0

def limpiar_link(link: Optional[str]) -> str:
    """
    Limpia y normaliza un enlace. Elimina espacios en blanco y caracteres no ASCII imprimibles.

    Args:
        link (Optional[str]): El enlace a limpiar.

    Returns:
        str: El enlace limpio y normalizado, o una cadena vacía si el enlace es None.
    """
    if not link:
        return ""
    try:
        # Se asegura de que solo queden caracteres ASCII imprimibles
        link_limpio = ''.join(c for c in link.strip() if c.isascii() and c.isprintable())
        return link_limpio
    except Exception as e:
        metricas.warning(f"Error limpiando link '{link}': {e}")
        return ""

def contiene_negativos(texto: str) -> bool:
    """
    Verifica si el texto de un anuncio contiene alguna de las palabras negativas definidas.

    Args:
        texto (str): El texto del anuncio.

    Returns:
        bool: True si el texto contiene alguna palabra negativa, False en caso contrario.
    """
    try:
        # Convertir a minúsculas una sola vez
        texto_lower = texto.lower()
        return any(palabra in texto_lower for palabra in PALABRAS_NEGATIVAS)
    except Exception as e:
        metricas.warning(f"Error verificando palabras negativas en texto: '{texto[:50]}...': {e}")
        return False

def es_extranjero(texto: str) -> bool:
    """
    Verifica si el texto de un anuncio sugiere una ubicación extranjera.

    Args:
        texto (str): El texto del anuncio.

    Returns:
        bool: True si el texto sugiere una ubicación extranjera, False en caso contrario.
    """
    try:
        # Convertir a minúsculas una sola vez
        texto_lower = texto.lower()
        return any(lugar in texto_lower for lugar in LUGARES_EXTRANJEROS)
    except Exception as e:
        metricas.warning(f"Error verificando ubicación extranjera en texto: '{texto[:50]}...': {e}")
        return False

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    """
    Valida si un precio es coherente con el modelo y el año del vehículo,
    comparándolo con un rango esperado basado en precios de referencia.

    Args:
        precio (int): El precio del vehículo.
        modelo (str): El modelo del vehículo.
        anio (int): El año del vehículo.

    Returns:
        bool: True si el precio es coherente, False en caso contrario.
    """
    try:
        if not (Config.PRECIO_MIN_VALIDO <= precio <= Config.PRECIO_MAX_VALIDO):
            return False
        
        # Obtener precio de referencia; si no está, usar un valor por defecto.
        precio_ref = PRECIOS_POR_DEFECTO.get(modelo, 50000)
        
        # El rango de coherencia se ajusta para ser más flexible.
        # Por ejemplo, entre 20% y 250% del precio de referencia.
        return (0.2 * precio_ref) <= precio <= (2.5 * precio_ref)
        
    except Exception as e:
        metricas.warning(f"Error validando coherencia de precio ({precio}) para modelo '{modelo}', año {anio}: {e}")
        return False

def limpiar_precio(texto: str) -> int:
    """
    Extrae el precio numérico de un texto, limpiando caracteres no numéricos.
    Prioriza números que parecen precios (3 a 7 dígitos) y no años.

    Args:
        texto (str): El texto que contiene el precio.

    Returns:
        int: El precio extraído, o 0 si no se encuentra un precio válido.
    """
    try:
        # Normalizar el texto: convertir a minúsculas y quitar caracteres no deseados
        s = re.sub(r"[Qq\$\.,]", "", texto.lower())
        
        # Buscar secuencias de 3 a 7 dígitos que podrían ser precios
        # Se excluyen años obvios (entre 1990 y el año actual + 1)
        # Se permite un pequeño margen para el año actual + 1 para capturar anuncios de modelos futuros.
        matches = re.findall(r"\b\d{3,7}\b", s)
        candidatos = [int(x) for x in matches if not (Config.AÑO_MIN_VALIDO <= int(x) <= AÑO_ACTUAL + 1)]
        
        if candidatos:
            # Se podría considerar la lógica para elegir el "mejor" candidato si hay varios,
            # pero por ahora se mantiene el primero encontrado.
            metricas.incrementar("precio_extraido")
            return candidatos[0]
        else:
            metricas.incrementar("precio_no_encontrado")
            return 0
            
    except Exception as e:
        metricas.warning(f"Error extrayendo precio del texto: '{texto[:50]}...': {e}")
        return 0

def filtrar_outliers(precios: List[int]) -> List[int]:
    """
    Filtra valores atípicos (outliers) de una lista de precios usando el método del Rango Intercuartílico (IQR).

    Args:
        precios (List[int]): Lista de precios a filtrar.

    Returns:
        List[int]: Lista de precios después de eliminar los outliers.
    """
    if len(precios) < 4:
        # Se necesitan al menos 4 puntos para calcular cuartiles de manera significativa.
        return precios
        
    try:
        # Calcular Q1 y Q3
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        
        # Calcular los límites para identificar outliers
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
        
        # Filtrar precios dentro de los límites
        filtrados = [p for p in precios if lim_inf <= p <= lim_sup]
        
        # Si después de filtrar quedan muy pocos datos, es mejor devolver la lista original
        # para no perder información valiosa si la muestra es pequeña o los "outliers" son representativos.
        if len(filtrados) >= Config.MUESTRA_MINIMA_MEDIA: # Usar MUESTRA_MINIMA_MEDIA como umbral
            metricas.incrementar("outliers_filtrados", len(precios) - len(filtrados))
            return filtrados
        else:
            return precios
            
    except Exception as e:
        metricas.warning(f"Error filtrando outliers en lista de precios: {e}")
        return precios

def coincide_modelo(texto: str, modelo: str) -> bool:
    """
    Verifica si el texto de un anuncio contiene el modelo especificado o alguno de sus sinónimos,
    realizando una normalización de texto para mejorar la coincidencia.

    Args:
        texto (str): El texto del anuncio.
        modelo (str): El modelo de vehículo a buscar.

    Returns:
        bool: True si se encuentra una coincidencia, False en caso contrario.
    """
    try:
        # Normalizar el texto del anuncio para ignorar acentos y caracteres especiales.
        # Se normaliza a NFKD y se decodifica a ASCII para eliminar tildes.
        texto_normalizado = unicodedata.normalize("NFKD", texto.lower()).encode("ascii", "ignore").decode("ascii")
        
        modelo_l = modelo.lower()
        # Combinar el modelo base con sus sinónimos.
        variantes = SINONIMOS_MODELO.get(modelo_l, []) + [modelo_l]
        
        # Verificar si alguna de las variantes está presente en el texto normalizado.
        coincide = any(variante in texto_normalizado for variante in variantes)
        if coincide:
            metricas.incrementar("modelo_coincidido")
        
        return coincide
        
    except Exception as e:
        metricas.warning(f"Error verificando coincidencia de modelo '{modelo}' en texto: '{texto[:50]}...': {e}")
        return False

def extraer_anio(texto: str, anio_actual: Optional[int] = None) -> Optional[int]:
    """
    Extrae el año del texto del anuncio, utilizando múltiples patrones y validaciones.

    Args:
        texto (str): El texto del anuncio.
        anio_actual (Optional[int]): El año actual. Si es None, se usa AÑO_ACTUAL.

    Returns:
        Optional[int]: El año extraído como entero, o None si no se encuentra un año válido.
    """
    if anio_actual is None:
        anio_actual = AÑO_ACTUAL

    try:
        texto_lower = texto.lower()
        
        # 1. Detectar años de 2 dígitos tipo "modelo 98"
        match_modelo = re.search(r"(modelo|año|año:?)\s?(\d{2})\b", texto_lower)
        if match_modelo:
            anio_2_digitos = int(match_modelo.group(2))
            # Heurística para convertir 2 dígitos a 4: 90-99 -> 19XX, 00-XX -> 20XX
            resultado = 1900 + anio_2_digitos if anio_2_digitos >= 90 else 2000 + anio_2_digitos
            if Config.AÑO_MIN_VALIDO <= resultado <= anio_actual:
                metricas.incrementar("anio_extraido_2digitos")
                return resultado

        # 2. Filtrar frases irrelevantes que contienen años para evitar falsos positivos
        patrones_ignorar = [
            r"se unió a facebook en \d{4}",
            r"miembro desde \d{4}",
            r"en facebook desde \d{4}",
            r"perfil creado en \d{4}",
        ]
        for patron in patrones_ignorar:
            texto_lower = re.sub(patron, '', texto_lower)

        # 3. Extraer años de 4 dígitos válidos
        # Patrón para 19XX o 20XX hasta el año actual (más un pequeño margen para modelos futuros).
        # Se extiende el rango superior a (anio_actual + 2) para mayor robustez.
        posibles_anios_str = re.findall(r"\b(19\d{2}|20\d{2})\b", texto_lower)
        # Convertir a enteros y filtrar por el rango válido
        candidatos_4_digitos = [
            int(a) for a in posibles_anios_str
            if Config.AÑO_MIN_VALIDO <= int(a) <= anio_actual + 2 # Rango ligeramente más amplio para capturar lo último
        ]
        
        # Devolver el año más reciente si hay múltiples candidatos válidos
        if candidatos_4_digitos:
            metricas.incrementar("anio_extraido_4digitos")
            return max(candidatos_4_digitos)

        metricas.incrementar("anio_no_encontrado")
        return None
        
    except Exception as e:
        metricas.warning(f"Error extrayendo año del texto: '{texto[:50]}...': {e}")
        return None

def validar_coherencia_precio_año(precio: int, anio: int) -> bool:
    """
    Valida la coherencia entre el precio de un vehículo y su año de fabricación.
    Define rangos de precios mínimos esperados para ciertos grupos de años.

    Args:
        precio (int): El precio del vehículo.
        anio (int): El año de fabricación del vehículo.

    Returns:
        bool: True si la combinación precio-año es coherente, False en caso contrario.
    """
    try:
        # Se usan umbrales más claros para la validación.
        # Estos valores pueden ser afinados o parametrizados en Config si se requiere flexibilidad.
        if anio >= 2020 and precio < 100_000:
            return False
        if anio >= 2016 and precio < 50_000:
            return False
        if anio >= 2010 and precio < 30_000:
            return False
        return True
        
    except Exception as e:
        metricas.warning(f"Error validando coherencia precio ({precio})-año ({anio}): {e}")
        return False

@timeit
@lru_cache(maxsize=256) # Aumentar maxsize ya que se usan combinaciones de modelo/año/tolerancia
def get_precio_referencia_cached(modelo: str, anio: int, tolerancia: int) -> Tuple[int, str, int, str]:
    """
    Obtiene el precio de referencia de un modelo y año específicos desde la base de datos,
    utilizando una caché LRU para mejorar el rendimiento de consultas repetidas.

    Args:
        modelo (str): El modelo del vehículo.
        anio (int): El año del vehículo.
        tolerancia (int): La tolerancia en años para buscar anuncios similares.

    Returns:
        Tuple[int, str, int, str]: Una tupla que contiene:
            - El precio de referencia (int).
            - Nivel de confianza ('alta', 'media', 'baja', 'error').
            - Tamaño de la muestra utilizada (int).
            - Rango de precios de la muestra (str).
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT precio FROM anuncios 
                WHERE modelo=? AND ABS(anio - ?) <= ? AND precio > 0
                ORDER BY precio
            """, (modelo, anio, tolerancia))
            precios = [row[0] for row in cur.fetchall()]
            
        if len(precios) >= Config.MUESTRA_MINIMA_CONFIABLE:
            # Si hay suficientes datos, filtrar outliers y calcular la mediana.
            precios_filtrados = filtrar_outliers(precios)
            median_price = int(statistics.median(precios_filtrados))
            return median_price, "alta", len(precios_filtrados), f"{min(precios_filtrados)}-{max(precios_filtrados)}"
        elif len(precios) >= Config.MUESTRA_MINIMA_MEDIA:
            # Si hay una muestra media, calcular la mediana sin filtrar outliers.
            median_price = int(statistics.median(precios))
            return median_price, "media", len(precios), f"{min(precios)}-{max(precios)}"
        else:
            # Si la muestra es insuficiente, usar el precio por defecto.
            default_price = PRECIOS_POR_DEFECTO.get(modelo, 50000)
            return default_price, "baja", 0, "default"
            
    except Exception as e:
        metricas.error(f"Error obteniendo precio de referencia para '{modelo}' (año {anio}, tolerancia {tolerancia}).", e)
        # En caso de error, retornar un valor por defecto y marcar la confianza como 'error'.
        return PRECIOS_POR_DEFECTO.get(modelo, 50000), "error", 0, "error"

@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """
    Obtiene el precio de referencia para un modelo y año, utilizando la versión cacheada.

    Args:
        modelo (str): El modelo del vehículo.
        anio (int): El año del vehículo.
        tolerancia (Optional[int]): La tolerancia en años. Si es None, usa Config.TOLERANCIA_PRECIO_REF.

    Returns:
        Dict[str, Any]: Un diccionario con el precio de referencia, confianza, tamaño de muestra y rango.
    """
    tolerancia = tolerancia if tolerancia is not None else Config.TOLERANCIA_PRECIO_REF
    precio, confianza, muestra, rango = get_precio_referencia_cached(modelo, anio, tolerancia)
    
    return {
        "precio": precio,
        "confianza": confianza,
        "muestra": muestra,
        "rango": rango
    }

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, anio: int, costo_extra: int = 2000) -> Dict[str, Any]:
    """
    Calcula el Retorno de Inversión (ROI) real de un vehículo, considerando su depreciación anual.

    Args:
        modelo (str): El modelo del vehículo.
        precio_compra (int): El precio al que se compraría el vehículo.
        anio (int): El año de fabricación del vehículo.
        costo_extra (int): Costos adicionales asociados a la compra (ej. traspaso).

    Returns:
        Dict[str, Any]: Un diccionario con el ROI calculado y otros detalles relevantes.
    """
    try:
        # Obtener el precio de referencia para el modelo y año dados.
        ref_data = get_precio_referencia(modelo, anio)
        ref_precio = ref_data["precio"]
        
        # Calcular la antigüedad del vehículo en años.
        años_antiguedad = max(0, AÑO_ACTUAL - anio)
        
        # Calcular el factor de depreciación acumulada.
        factor_depreciacion = (1 - Config.DEPRECIACION_ANUAL) ** años_antiguedad
        
        # Calcular el precio depreciado basado en el precio de referencia.
        precio_depreciado = ref_precio * factor_depreciacion
        
        # Calcular la inversión total (precio de compra + costos extra).
        inversion_total = precio_compra + costo_extra
        
        # Calcular el ROI. Evitar división por cero.
        roi = ((precio_depreciado - inversion_total) / inversion_total) * 100 if inversion_total > 0 else 0.0
        
        metricas.incrementar("roi_calculado")
        
        return {
            "roi": round(roi, 1),
            "precio_referencia": ref_data["precio"],
            "precio_depreciado": int(precio_depreciado),
            "confianza": ref_data["confianza"],
            "muestra": ref_data["muestra"],
            "inversion_total": inversion_total,
            "años_antiguedad": años_antiguedad
        }
        
    except Exception as e:
        metricas.error(f"Error calculando ROI para modelo '{modelo}', precio {precio_compra}, año {anio}.", e)
        # En caso de error, retornar valores predeterminados.
        return {
            "roi": 0.0,
            "precio_referencia": PRECIOS_POR_DEFECTO.get(modelo, 50000),
            "precio_depreciado": 0,
            "confianza": "error",
            "muestra": 0,
            "inversion_total": precio_compra + costo_extra,
            "años_antiguedad": 0
        }

@timeit
def puntuar_anuncio(texto: str, roi_info: Optional[Dict] = None) -> int:
    """
    Calcula una puntuación para un anuncio basado en varios factores
    como el modelo, año, precio y ROI.

    Args:
        texto (str): El texto completo del anuncio.
        roi_info (Optional[Dict]): Información precalculada del ROI.
                                   Si es None, se calcula dentro de la función.

    Returns:
        int: La puntuación del anuncio (entre 0 y 10).
    """
    try:
        precio = limpiar_precio(texto)
        anio = extraer_anio(texto)
        
        # Encontrar el primer modelo de interés que coincide con el texto.
        modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
        
        # Si falta información clave, no se puede puntuar.
        if not (modelo and anio and precio):
            metricas.incrementar("anuncio_no_puntuado_info_faltante")
            return 0
            
        # Validar coherencia básica de precio y año.
        if not validar_precio_coherente(precio, modelo, anio):
            metricas.incrementar("anuncio_no_puntuado_incoherente")
            return 0
            
        # Si no se proporciona roi_info, calcularlo.
        if roi_info is None:
            roi_info = calcular_roi_real(modelo, precio, anio)
        roi = roi_info["roi"]
        
        score = 4  # Score base
        
        # Bonificación/penalización por ROI
        if roi >= 25: score += 4
        elif roi >= 15: score += 3
        elif roi >= 10: score += 2
        elif roi >= 5: score += 1
        else: score -= 1 # Penalización si el ROI es bajo o negativo
        
        # Bonificación por precio bajo (puede indicar una buena oferta)
        if precio <= 25000: score += 2
        elif precio <= 35000: score += 1
        
        # Bonificación por descripción detallada (más información = más confianza)
        if len(texto.split()) >= 8: score += 1
        
        # Asegurar que el score final esté en el rango [0, 10]
        score_final = max(0, min(score, 10))
        metricas.incrementar("anuncio_puntuado")
        
        return score_final
        
    except Exception as e:
        metricas.error(f"Error puntuando anuncio con texto: '{texto[:50]}...'", e)
        return 0

@timeit
def insertar_anuncio_db(link: str, modelo: str, anio: int, precio: int, km: str, roi: float, score: int,
                        relevante: bool = False, confianza_precio: Optional[str] = None, muestra_precio: Optional[int] = None):
    """
    Inserta un anuncio en la base de datos (función legada).
    Esta función es menos preferida que `insertar_anuncio_en_db` o `insertar_lote_anuncios`
    debido a su gestión de conexión manual y lógica de actualización de columnas.

    Args:
        link (str): El enlace del anuncio.
        modelo (str): Modelo del vehículo.
        anio (int): Año del vehículo.
        precio (int): Precio del vehículo.
        km (str): Kilometraje del vehículo.
        roi (float): Retorno de Inversión.
        score (int): Puntuación del anuncio.
        relevante (bool): Si el anuncio es relevante.
        confianza_precio (Optional[str]): Nivel de confianza del precio.
        muestra_precio (Optional[int]): Tamaño de la muestra de precios.
    """
    try:
        conn = get_conn() # Obtiene la conexión legada
        cur = conn.cursor()
        
        # Verificar si las nuevas columnas existen antes de intentar usarlas.
        # Esto asegura compatibilidad con esquemas de BD antiguos.
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}

        if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]):
            cur.execute("""
            INSERT OR REPLACE INTO anuncios 
            (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'), DATE('now'))
            """, (link, modelo, anio, precio, km, roi, score, int(relevante), confianza_precio, muestra_precio))
        else:
            # Versión de inserción para esquemas de tabla antiguos sin las nuevas columnas.
            cur.execute("""
            INSERT OR REPLACE INTO anuncios 
            (link, modelo, anio, precio, km, roi, score, fecha_scrape, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, DATE('now'), DATE('now'))
            """, (link, modelo, anio, precio, km, roi, score))

        conn.commit()
        metricas.incrementar("anuncio_insertado_legacy")
        
    except Exception as e:
        metricas.error(f"Error insertando anuncio (legacy) con link: {link}.", e)
        raise

def existe_en_db(link: str) -> bool:
    """
    Verifica si un anuncio con un determinado enlace ya existe en la base de datos.

    Args:
        link (str): El enlace del anuncio a verificar.

    Returns:
        bool: True si el anuncio existe, False en caso contrario.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Se limpia el link antes de la consulta para asegurar consistencia
            cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
            existe = cur.fetchone() is not None
            
            if existe:
                metricas.incrementar("anuncio_ya_existe")
            
            return existe
            
    except Exception as e:
        metricas.error(f"Error verificando existencia de anuncio con link: {link}.", e)
        return False

@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    """
    Calcula el rendimiento de un modelo de vehículo en los últimos días,
    basado en la proporción de anuncios con un score aceptable.

    Args:
        modelo (str): El modelo de vehículo.
        dias (int): El número de días hacia atrás para considerar el rendimiento.

    Returns:
        float: El rendimiento del modelo (0.0 a 1.0), o 0.0 si hay un error o no hay datos.
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0, 
                    COUNT(*)
                FROM anuncios 
                WHERE modelo = ? AND fecha_scrape >= date('now', ?)
            """, (Config.SCORE_MIN_DB, modelo, f"-{dias} days"))
            
            # Fetchone() devolverá una tupla (count_relevant, total_count)
            relevant_count, total_count = cur.fetchone()
            
            if total_count and total_count > 0:
                rendimiento = relevant_count / total_count
            else:
                rendimiento = 0.0 # No hay anuncios para calcular el rendimiento
            
            return round(rendimiento, 3)
            
    except Exception as e:
        metricas.error(f"Error calculando rendimiento para modelo '{modelo}' en {dias} días.", e)
        return 0.0

@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    """
    Identifica los modelos de vehículos que están mostrando un rendimiento bajo
    (es decir, pocos anuncios con score alto) en los últimos días.

    Args:
        threshold (float): El umbral de rendimiento por debajo del cual un modelo se considera de bajo rendimiento.
        dias (int): El número de días para calcular el rendimiento.

    Returns:
        List[str]: Una lista de modelos de vehículos con bajo rendimiento.
    """
    return [modelo for modelo in MODELOS_INTERES if get_rendimiento_modelo(modelo, dias) < threshold]

def get_estadisticas_db() -> Dict[str, Any]:
    """
    Obtiene estadísticas generales de la base de datos de anuncios.

    Returns:
        Dict[str, Any]: Un diccionario con el total de anuncios, conteo por confianza de precio
                        y conteo de anuncios por modelo.
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Total de anuncios
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total_anuncios = cur.fetchone()[0]
        
        # Verificar la existencia de columnas para compatibilidad
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}
        
        alta_conf = 0
        baja_conf = 0
        if "confianza_precio" in columnas_existentes:
            cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'alta'")
            alta_conf = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'baja'")
            baja_conf = cur.fetchone()[0]
        else:
            # Si la columna no existe, se asume que toda la confianza es 'baja'
            baja_conf = total_anuncios
        
        # Anuncios por modelo
        cur.execute("""
            SELECT modelo, COUNT(*) FROM anuncios 
            GROUP BY modelo ORDER BY COUNT(*) DESC
        """)
        anuncios_por_modelo = dict(cur.fetchall())
        
        return {
            "total_anuncios": total_anuncios,
            "alta_confianza_precio": alta_conf,
            "baja_confianza_precio": baja_conf,
            "porcentaje_baja_confianza": round((baja_conf / total_anuncios) * 100, 1) if total_anuncios else 0,
            "anuncios_por_modelo": anuncios_por_modelo
        }

def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    """
    Analiza un mensaje de texto para extraer información de un anuncio de vehículo
    y calcular su ROI y puntuación.

    Args:
        texto (str): El mensaje de texto a analizar.

    Returns:
        Optional[Dict[str, Any]]: Un diccionario con la información del anuncio si se puede analizar,
                                   o None si no se encuentra información suficiente o es inválida.
    """
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    
    # Si falta cualquier pieza clave de información, no se puede analizar.
    if not (modelo and anio and precio):
        logger.debug(f"Mensaje no pudo ser analizado: falta modelo, año o precio. Texto: '{texto[:50]}...'")
        metricas.incrementar("mensaje_no_analizado")
        return None
        
    # Realizar una validación de coherencia básica antes de cálculos más complejos.
    if not validar_precio_coherente(precio, modelo, anio):
        logger.debug(f"Mensaje no analizado: precio/año incoherente. Modelo: {modelo}, Año: {anio}, Precio: {precio}")
        metricas.incrementar("mensaje_no_analizado_incoherente")
        return None
        
    # Calcular ROI y puntuación.
    roi_data = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio(texto, roi_data)
    
    # Extraer URL si existe.
    # El patrón de regex para URLs podría ser más robusto, pero se mantiene la lógica existente.
    url_match = re.search(r"https?://\S+", texto)
    url = limpiar_link(url_match.group(0)) if url_match else ""

    # Determinar si el anuncio es relevante basado en score y ROI.
    relevante = score >= Config.SCORE_MIN_TELEGRAM and roi_data["roi"] >= Config.ROI_MINIMO
    
    metricas.incrementar("mensaje_analizado")
    return {
        "url": url,
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "roi": roi_data["roi"],
        "score": score,
        "relevante": relevante,
        "km": "", # El código original no extrae KM, se mantiene como cadena vacía.
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data # Se incluye toda la data de ROI para posibles usos futuros.
    }
