# Utils_analisis.py

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

# Configuración centralizada
class Config:
    """Configuración centralizada del sistema de análisis de anuncios"""
    
    # Base de datos
    DB_PATH = os.path.abspath(os.environ.get("DB_PATH", "upload-artifact/anuncios.db"))
    
    # Scoring y filtros
    SCORE_MIN_DB = 4
    SCORE_MIN_TELEGRAM = 6
    ROI_MINIMO = 10.0
    TOLERANCIA_PRECIO_REF = 1
    DEPRECIACION_ANUAL = 0.08
    MUESTRA_MINIMA_CONFIABLE = 5
    MUESTRA_MINIMA_MEDIA = 2
    
    # Validación
    PRECIO_MIN_VALIDO = 5000
    PRECIO_MAX_VALIDO = 500000
    AÑO_MIN_VALIDO = 1990
    
    # Debug
    DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")

# Configuración de logging
logging.basicConfig(
    level=logging.INFO if Config.DEBUG else logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Constantes
AÑO_ACTUAL = datetime.now().year

# Precios de referencia por modelo (en quetzales)
PRECIOS_POR_DEFECTO = {
    "yaris": 45000, "civic": 65000, "corolla": 50000, "sentra": 42000,
    "rav4": 130000, "cr-v": 95000, "tucson": 70000, "kia picanto": 35000,
    "chevrolet spark": 30000, "nissan march": 37000, "suzuki alto": 26000,
    "suzuki swift": 40000, "hyundai accent": 41000, "mitsubishi mirage": 33000,
    "suzuki grand vitara": 52000, "hyundai i10": 34000, "kia rio": 40000,
    "toyota": 48000, "honda": 50000
}

# Modelos de interés para el análisis
MODELOS_INTERES = list(PRECIOS_POR_DEFECTO.keys())

# Palabras que indican que el vehículo no está en buenas condiciones
PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "motor fundido", "no arranca", "no enciende", "papeles atrasados",
    "sin motor", "para partes", "no funciona", "accidentado"
]

# Ubicaciones extranjeras que debemos filtrar
LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Sinónimos para mejorar la coincidencia de modelos
SINONIMOS_MODELO = {
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

# Métricas de sesión para monitoreo
class MetricasSession:
    """Clase para recopilar métricas de la sesión actual"""
    
    def __init__(self):
        self.inicio = datetime.now()
        self.contadores = defaultdict(int)
        self.errores = []
        self.warnings = []
    
    def incrementar(self, metrica: str, cantidad: int = 1):
        """Incrementar contador de una métrica específica"""
        self.contadores[metrica] += cantidad
    
    def error(self, mensaje: str, excepcion: Optional[Exception] = None):
        """Registrar un error en el sistema"""
        self.errores.append({
            "timestamp": datetime.now(),
            "mensaje": mensaje,
            "excepcion": str(excepcion) if excepcion else None
        })
        logger.error(f"{mensaje}: {excepcion}" if excepcion else mensaje)
    
    def warning(self, mensaje: str):
        """Registrar una advertencia en el sistema"""
        self.warnings.append({
            "timestamp": datetime.now(),
            "mensaje": mensaje
        })
        logger.warning(mensaje)
    
    def resumen(self) -> Dict[str, Any]:
        """Generar resumen de métricas de la sesión"""
        duracion = datetime.now() - self.inicio
        return {
            "duracion_segundos": duracion.total_seconds(),
            "contadores": dict(self.contadores),
            "total_errores": len(self.errores),
            "total_warnings": len(self.warnings),
            "errores": self.errores[-5:],  # Solo últimos 5 errores
            "warnings": self.warnings[-5:]  # Solo últimos 5 warnings
        }

# Instancia global de métricas
metricas = MetricasSession()

def escapar_multilinea(texto: str) -> str:
    """Escapar caracteres especiales para Telegram MarkdownV2"""
    if not texto:
        return ""
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

# Inicialización de directorio de base de datos
os.makedirs(os.path.dirname(Config.DB_PATH), exist_ok=True)

def timeit(func):
    """Decorador para medir tiempo de ejecución de funciones"""
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
    """Context manager para manejo seguro de conexiones de base de datos"""
    conn = None
    try:
        conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        metricas.error(f"Error en transacción de BD", e)
        raise
    finally:
        if conn:
            conn.close()

# Pool de conexiones simple para compatibilidad
_conn: Optional[sqlite3.Connection] = None

def get_conn():
    """Obtener conexión reutilizable (función legacy para compatibilidad)"""
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
    return _conn

@timeit
def inicializar_tabla_anuncios():
    """Inicializar tabla de anuncios con todas las columnas necesarias"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()

            # Verificar si la tabla existe
            cur.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='anuncios'
            """)
            
            if cur.fetchone() is None:
                # Crear tabla completa con índices para optimización
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
                
                # Crear índices para mejorar rendimiento de consultas
                cur.execute("CREATE INDEX idx_modelo_anio ON anuncios(modelo, anio)")
                cur.execute("CREATE INDEX idx_fecha_scrape ON anuncios(fecha_scrape)")
                cur.execute("CREATE INDEX idx_score ON anuncios(score)")
                cur.execute("CREATE INDEX idx_relevante ON anuncios(relevante)")
                
                logger.info("✅ Tabla anuncios creada con estructura completa e índices")
            else:
                # Agregar columnas faltantes si es necesario
                cur.execute("PRAGMA table_info(anuncios)")
                cols_existentes = {row[1] for row in cur.fetchall()}
                
                columnas_necesarias = [
                    ("updated_at", "DATE DEFAULT DATE('now')"),
                    ("relevante", "INTEGER DEFAULT 0"),
                    ("confianza_precio", "TEXT DEFAULT 'baja'"),
                    ("muestra_precio", "INTEGER DEFAULT 0")
                ]
                
                for col_nombre, col_def in columnas_necesarias:
                    if col_nombre not in cols_existentes:
                        try:
                            cur.execute(f"ALTER TABLE anuncios ADD COLUMN {col_nombre} {col_def}")
                            logger.info(f"✅ Columna '{col_nombre}' agregada")
                        except sqlite3.OperationalError as e:
                            metricas.warning(f"No se pudo agregar columna {col_nombre}: {e}")

            conn.commit()
            metricas.incrementar("tabla_inicializada")
            
    except Exception as e:
        metricas.error("Error inicializando tabla", e)
        raise

def validar_anuncio_completo(texto: str, precio: int, anio: int, modelo: str) -> Tuple[bool, str]:
    """Validación centralizada de anuncios con razón específica de rechazo"""
    if not texto or not modelo:
        return False, "datos_incompletos"
    
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
    """Insertar múltiples anuncios en una sola transacción para mejor rendimiento"""
    if not anuncios:
        return {"nuevos": 0, "actualizados": 0, "errores": 0}
    
    contadores = {"nuevos": 0, "actualizados": 0, "errores": 0}
    
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            for anuncio in anuncios:
                try:
                    # Validar campos requeridos
                    if not all(k in anuncio for k in ["link", "modelo", "anio", "precio"]):
                        contadores["errores"] += 1
                        continue
                    
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
                        anuncio["precio"], anuncio.get("km", ""), anuncio.get("roi", 0.0),
                        anuncio.get("score", 0), int(anuncio.get("relevante", False)),
                        anuncio.get("confianza_precio", "baja"), anuncio.get("muestra_precio", 0)
                    ))
                    
                    if cur.lastrowid:
                        contadores["nuevos"] += 1
                    else:
                        contadores["actualizados"] += 1
                        
                except sqlite3.Error as e:
                    contadores["errores"] += 1
                    metricas.error(f"Error insertando anuncio {anuncio.get('link', 'unknown')}", e)
            
            conn.commit()
            metricas.incrementar("lote_insertado")
            
    except Exception as e:
        metricas.error("Error en inserción de lote", e)
        raise
    
    return contadores

def insertar_o_actualizar_anuncio_db(
    conn,
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
    """Insertar o actualizar un anuncio individual y devolver el tipo de operación"""
    if not link or not modelo:
        raise ValueError("Link y modelo son campos requeridos")
    
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
            int(relevante), confianza_precio, muestra_precio
        ))
        conn.commit()
        
        resultado = "nuevo" if cur.lastrowid else "actualizado"
        metricas.incrementar(f"anuncio_{resultado}")
        return resultado
        
    except sqlite3.Error as e:
        metricas.error(f"Error insertando/actualizando anuncio {link}", e)
        raise

def limpiar_anuncios_antiguos(dias: int = 30) -> int:
    """Eliminar anuncios antiguos para mantener la base de datos limpia"""
    if dias <= 0:
        raise ValueError("Los días deben ser un número positivo")
    
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM anuncios 
                WHERE fecha_scrape < date('now', '-{} days')
            """.format(dias))
            eliminados = cur.rowcount
            conn.commit()
            
            if eliminados > 0:
                logger.info(f"🗑️ Eliminados {eliminados} anuncios antiguos (>{dias} días)")
            
            metricas.incrementar("anuncios_eliminados", eliminados)
            return eliminados
            
    except Exception as e:
        metricas.error("Error limpiando anuncios antiguos", e)
        return 0

def limpiar_link(link: Optional[str]) -> str:
    """Limpiar y normalizar enlaces para evitar caracteres problemáticos"""
    if not link:
        return ""
    try:
        # Remover caracteres no ASCII y no imprimibles
        link_limpio = ''.join(c for c in link.strip() if c.isascii() and c.isprintable())
        return link_limpio
    except Exception as e:
        metricas.warning(f"Error limpiando link: {e}")
        return ""

def contiene_negativos(texto: str) -> bool:
    """Verificar si el texto contiene palabras que indican problemas con el vehículo"""
    if not texto:
        return False
    
    try:
        texto_lower = texto.lower()
        return any(palabra_negativa in texto_lower for palabra_negativa in PALABRAS_NEGATIVAS)
    except Exception as e:
        metricas.warning(f"Error verificando palabras negativas: {e}")
        return False

def es_extranjero(texto: str) -> bool:
    """Verificar si el anuncio proviene de una ubicación extranjera"""
    if not texto:
        return False
    
    try:
        texto_lower = texto.lower()
        return any(lugar in texto_lower for lugar in LUGARES_EXTRANJEROS)
    except Exception as e:
        metricas.warning(f"Error verificando ubicación extranjera: {e}")
        return False

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    """Validar que el precio sea coherente con el modelo y año del vehículo"""
    if precio <= 0:
        return False
    
    try:
        if precio < Config.PRECIO_MIN_VALIDO or precio > Config.PRECIO_MAX_VALIDO:
            return False
        
        precio_ref = PRECIOS_POR_DEFECTO.get(modelo, 50000)
        # Permitir rango de 20% a 250% del precio de referencia
        precio_min = 0.2 * precio_ref
        precio_max = 2.5 * precio_ref
        
        return precio_min <= precio <= precio_max
        
    except Exception as e:
        metricas.warning(f"Error validando precio coherente: {e}")
        return False

def limpiar_precio(texto: str) -> int:
    """Extraer precio numérico del texto del anuncio"""
    if not texto:
        return 0
    
    try:
        # Remover caracteres comunes en precios
        texto_limpio = re.sub(r"[Qq\$\.,]", "", texto.lower())
        
        # Buscar números de 3 a 7 dígitos que no sean años
        matches = re.findall(r"\b\d{3,7}\b", texto_limpio)
        candidatos = [
            int(match) for match in matches 
            if int(match) >= 3000 and (int(match) < 1990 or int(match) > AÑO_ACTUAL + 1)
        ]
        
        if candidatos:
            metricas.incrementar("precio_extraido")
            return candidatos[0]
        else:
            metricas.incrementar("precio_no_encontrado")
            return 0
            
    except Exception as e:
        metricas.warning(f"Error extrayendo precio: {e}")
        return 0

def filtrar_outliers(precios: List[int]) -> List[int]:
    """Filtrar valores atípicos usando el método del rango intercuartílico (IQR)"""
    if len(precios) < 4:
        return precios
        
    try:
        # Calcular cuartiles
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        
        # Definir límites para outliers
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
        
        # Filtrar valores dentro del rango
        filtrados = [precio for precio in precios if lim_inf <= precio <= lim_sup]
        
        if len(filtrados) >= 2:
            outliers_removidos = len(precios) - len(filtrados)
            metricas.incrementar("outliers_filtrados", outliers_removidos)
            return filtrados
        else:
            return precios
            
    except Exception as e:
        metricas.warning(f"Error filtrando outliers: {e}")
        return precios

def coincide_modelo(texto: str, modelo: str) -> bool:
    """Verificar si el texto coincide con un modelo específico usando sinónimos"""
    if not texto or not modelo:
        return False
    
    try:
        # Normalizar texto para comparación
        texto_normalizado = unicodedata.normalize("NFKD", texto.lower())
        modelo_lower = modelo.lower()
        
        # Obtener variantes del modelo incluyendo sinónimos
        variantes = SINONIMOS_MODELO.get(modelo_lower, []) + [modelo_lower]
        
        # Convertir a ASCII para mejor comparación
        texto_ascii = unicodedata.normalize("NFKD", texto_normalizado).encode("ascii", "ignore").decode("ascii")
        
        # Verificar si alguna variante está en el texto
        coincide = any(variante in texto_ascii for variante in variantes)
        
        if coincide:
            metricas.incrementar("modelo_coincidido")
        
        return coincide
        
    except Exception as e:
        metricas.warning(f"Error verificando coincidencia de modelo: {e}")
        return False

def extraer_anio(texto: str, anio_actual: int = None) -> Optional[int]:
    """Extraer año del vehículo del texto usando múltiples patrones de búsqueda"""
    if not texto:
        return None
    
    if anio_actual is None:
        anio_actual = AÑO_ACTUAL

    try:
        texto_lower = texto.lower()
        
        # 1. Detectar años de 2 dígitos con contexto (ej: "modelo 98")
        match_modelo = re.search(r"(modelo|año)\s?(\d{2})\b", texto_lower)
        if match_modelo:
            anio_2_digitos = int(match_modelo.group(2))
            # Convertir a año completo asumiendo que 90-99 son 1990-1999
            anio_completo = 1900 + anio_2_digitos if anio_2_digitos >= 90 else 2000 + anio_2_digitos
            
            if Config.AÑO_MIN_VALIDO <= anio_completo <= anio_actual:
                metricas.incrementar("anio_extraido_2digitos")
                return anio_completo

        # 2. Filtrar frases irrelevantes que contienen años
        patrones_ignorar = [
            r"se unió a facebook en \d{4}",
            r"miembro desde \d{4}",
            r"en facebook desde \d{4}",
            r"perfil creado en \d{4}",
        ]
        
        for patron in patrones_ignorar:
            texto_lower = re.sub(patron, '', texto_lower)

        # 3. Buscar años de 4 dígitos válidos
        años_encontrados = re.findall(r"\b(19\d{2}|20[0-3]\d)\b", texto_lower)
        for anio_str in años_encontrados:
            anio_int = int(anio_str)
            if Config.AÑO_MIN_VALIDO <= anio_int <= anio_actual:
                metricas.incrementar("anio_extraido_4digitos")
                return anio_int

        metricas.incrementar("anio_no_encontrado")
        return None
        
    except Exception as e:
        metricas.warning(f"Error extrayendo año: {e}")
        return None

def validar_coherencia_precio_año(precio: int, anio: int) -> bool:
    """Validar que el precio sea coherente con el año del vehículo"""
    if precio <= 0 or anio <= 0:
        return False
    
    try:
        # Vehículos muy nuevos no pueden ser muy baratos
        if anio >= 2020 and precio < 100_000:
            return False
        
        # Vehículos relativamente nuevos tampoco
        if anio >= 2016 and precio < 50_000:
            return False
        
        # Vehículos de la década pasada tienen un mínimo razonable
        if anio >= 2010 and precio < 30_000:
            return False
        
        return True
        
    except Exception as e:
        metricas.warning(f"Error validando coherencia precio-año: {e}")
        return False

@timeit
@lru_cache(maxsize=128)
def get_precio_referencia_cached(modelo: str, anio: int, tolerancia: int) -> Tuple[int, str, int, str]:
    """Obtener precio de referencia del modelo con cache para mejor rendimiento"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT precio FROM anuncios 
                WHERE modelo=? AND ABS(anio - ?) <= ? AND precio > 0
                ORDER BY precio
            """, (modelo, anio, tolerancia))
            
            precios = [row[0] for row in cur.fetchall()]
            
        # Determinar confianza según el tamaño de la muestra
        if len(precios) >= Config.MUESTRA_MINIMA_CONFIABLE:
            precios_filtrados = filtrar_outliers(precios)
            mediana = statistics.median(precios_filtrados)
            rango = f"{min(precios_filtrados)}-{max(precios_filtrados)}"
            return int(mediana), "alta", len(precios_filtrados), rango
            
        elif len(precios) >= Config.MUESTRA_MINIMA_MEDIA:
            mediana = statistics.median(precios)
            rango = f"{min(precios)}-{max(precios)}"
            return int(mediana), "media", len(precios), rango
            
        else:
            # Usar precio por defecto si no hay suficientes datos
            precio_default = PRECIOS_POR_DEFECTO.get(modelo, 50000)
            return precio_default, "baja", 0, "default"
            
    except Exception as e:
        metricas.error(f"Error obteniendo precio referencia para {modelo}", e)
        precio_default = PRECIOS_POR_DEFECTO.get(modelo, 50000)
        return precio_default, "baja", 0, "error"

@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """Obtener información completa del precio de referencia"""
    if not modelo or anio <= 0:
        raise ValueError("Modelo y año son requeridos y deben ser válidos")
    
    tolerancia = tolerancia or Config.TOLERANCIA_PRECIO_REF
    precio, confianza, muestra, rango = get_precio_referencia_cached(modelo, anio, tolerancia)
    
    return {
        "precio": precio,
        "confianza": confianza,
        "muestra": muestra,
        "rango": rango
    }

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, anio: int, costo_extra: int = 2000) -> Dict[str, Any]:
    """Calcular ROI real considerando depreciación del vehículo"""
    if not modelo or precio_compra <= 0 or anio <= 0:
        raise ValueError("Todos los parámetros deben ser válidos")
    
    try:
        # Obtener precio de referencia
        ref_info = get_precio_referencia(modelo, anio)
        
        # Calcular depreciación por edad
        años_antiguedad = max(0, AÑO_ACTUAL - anio)
        factor_depreciacion = (1 - Config.DEPRECIACION_ANUAL) ** años_antiguedad
        precio_depreciado = ref_info["precio"] * factor_depreciacion
        
        # Calcular ROI
        inversion_total = precio_compra + costo_extra
        roi = ((precio_depreciado - inversion_total) / inversion_total) * 100 if inversion_total > 0 else 0.0

        
    except Exception as e:
        roi = 0.0
        
        metricas.incrementar("roi_calculado")

        
        return {
            "roi": round(roi, 1),
            "precio_referencia": ref_info["precio"],
            "precio_depreciado": int(precio_depreciado),
            "confianza": ref_info["confianza"],
            "muestra": ref_info["muestra"],
            "inversion_total": inversion_total,
            "años_antiguedad": años_antiguedad
        }
        
    except Exception as e:
        metricas.error(f"Error calculando ROI para {modelo}", e)
        return {
            "roi": 0.0,
            "precio_referencia": PRECIOS_POR_DEFECTO.get(modelo, 50000),
            "precio_depreciado": 0,
            "confianza": "error",
            "muestra": 0,
            "inversion_total": precio_compra + costo_extra,
            "años_antiguedad": 0
        }

def generar_reporte_detallado(modelo: str = None, dias: int = 7) -> Dict[str, Any]:
    """Generar reporte detallado de análisis de anuncios"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Filtro por modelo si se especifica
            filtro_modelo = "AND modelo = ?" if modelo else ""
            params = [dias]
            if modelo:
                params.append(modelo)
            
            # Estadísticas generales
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN score >= ? THEN 1 END) as relevantes,
                    AVG(roi) as roi_promedio,
                    AVG(score) as score_promedio,
                    MIN(precio) as precio_min,
                    MAX(precio) as precio_max,
                    AVG(precio) as precio_promedio
                FROM anuncios 
                WHERE fecha_scrape >= date('now', '-{dias} days') {filtro_modelo}
            """, [Config.SCORE_MIN_DB] + params)
            
            stats = cur.fetchone()
            
            # Top modelos por rendimiento
            cur.execute(f"""
                SELECT modelo, 
                       COUNT(*) as total,
                       COUNT(CASE WHEN score >= ? THEN 1 END) as relevantes,
                       ROUND(AVG(roi), 1) as roi_promedio
                FROM anuncios 
                WHERE fecha_scrape >= date('now', '-{dias} days')
                GROUP BY modelo 
                ORDER BY relevantes DESC, roi_promedio DESC
                LIMIT 10
            """, [Config.SCORE_MIN_DB])
            
            top_modelos = cur.fetchall()
            
            return {
                "periodo_dias": dias,
                "modelo_filtro": modelo,
                "estadisticas": {
                    "total_anuncios": stats[0] or 0,
                    "anuncios_relevantes": stats[1] or 0,
                    "roi_promedio": round(stats[2] or 0, 1),
                    "score_promedio": round(stats[3] or 0, 1),
                    "precio_min": stats[4] or 0,
                    "precio_max": stats[5] or 0,
                    "precio_promedio": round(stats[6] or 0, 0)
                },
                "top_modelos": [
                    {
                        "modelo": row[0],
                        "total": row[1],
                        "relevantes": row[2],
                        "roi_promedio": row[3]
                    }
                    for row in top_modelos
                ],
                "metricas_session": metricas.resumen()
            }
            
    except Exception as e:
        metricas.error("Error generando reporte detallado", e)
        return {
            "error": str(e),
            "metricas_session": metricas.resumen()
        }

def optimizar_base_datos() -> Dict[str, Any]:
    """Optimizar base de datos para mejor rendimiento"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Recopilar estadísticas antes de optimizar
            cur.execute("SELECT COUNT(*) FROM anuncios")
            total_antes = cur.fetchone()[0]
            
            # Eliminar duplicados manteniendo el más reciente
            cur.execute("""
                DELETE FROM anuncios 
                WHERE rowid NOT IN (
                    SELECT MIN(rowid) 
                    FROM anuncios 
                    GROUP BY link
                )
            """)
            duplicados_eliminados = cur.rowcount
            
            # Vacuum para compactar base de datos
            cur.execute("VACUUM")
            
            # Analyze para actualizar estadísticas de consulta
            cur.execute("ANALYZE")
            
            # Recopilar estadísticas después
            cur.execute("SELECT COUNT(*) FROM anuncios")
            total_despues = cur.fetchone()[0]
            
            conn.commit()
            
            resultado = {
                "total_antes": total_antes,
                "total_despues": total_despues,
                "duplicados_eliminados": duplicados_eliminados,
                "optimizacion_completada": True
            }
            
            metricas.incrementar("optimizacion_bd")
            logger.info(f"🔧 BD optimizada: {duplicados_eliminados} duplicados eliminados")
            
            return resultado
            
    except Exception as e:
        metricas.error("Error optimizando base de datos", e)
        return {
            "error": str(e),
            "optimizacion_completada": False
        }

@timeit
def puntuar_anuncio(texto: str, roi_info: Optional[Dict] = None) -> int:
    """Puntuar anuncio basado en múltiples factores de calidad y rentabilidad"""
    try:
        # Extraer información básica del anuncio
        precio = limpiar_precio(texto)
        anio = extraer_anio(texto)
        modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
        
        # Verificar que tengamos datos mínimos
        if not (modelo and anio and precio):
            metricas.incrementar("puntuacion_datos_insuficientes")
            return 0
            
        # Validar coherencia de precio
        if not validar_precio_coherente(precio, modelo, anio):
            metricas.incrementar("puntuacion_precio_incoherente")
            return 0
            
        # Calcular ROI si no se proporciona
        if roi_info is None:
            roi_info = calcular_roi_real(modelo, precio, anio)
        
        roi = roi_info.get("roi", 0)
        
        # Score base
        score = 4
        
        # Bonificación por ROI (factor más importante)
        if roi >= 25:
            score += 4
        elif roi >= 15:
            score += 3
        elif roi >= 10:
            score += 2
        elif roi >= 5:
            score += 1
        elif roi < 0:
            score -= 2  # Penalización por ROI negativo
        
        # Bonificación por precio atractivo
        if precio <= 25000:
            score += 2
        elif precio <= 35000:
            score += 1
        elif precio >= 100000:
            score -= 1  # Penalización por precio muy alto
        
        # Bonificación por descripción detallada
        palabras = len(texto.split())
        if palabras >= 15:
            score += 2
        elif palabras >= 8:
            score += 1
        elif palabras < 4:
            score -= 1  # Penalización por descripción muy pobre
        
        # Bonificación por año reciente
        años_antiguedad = AÑO_ACTUAL - anio
        if años_antiguedad <= 5:
            score += 1
        elif años_antiguedad >= 15:
            score -= 1
        
        # Bonificación por confianza en el precio
        confianza = roi_info.get("confianza", "baja")
        if confianza == "alta":
            score += 1
        elif confianza == "error":
            score -= 1
        
        # Asegurar que el score esté en el rango válido
        score_final = max(0, min(score, 10))
        
        metricas.incrementar("anuncio_puntuado")
        
        if Config.DEBUG:
            logger.debug(f"Score {score_final} para {modelo} {anio}: ROI={roi}%, precio={precio}")
        
        return score_final
        
    except Exception as e:
        metricas.error(f"Error puntuando anuncio: {e}")
        return 0

def validar_integridad_datos() -> Dict[str, Any]:
    """Validar integridad de datos en la base de datos"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            problemas = []
            
            # Verificar registros con datos faltantes
            cur.execute("""
                SELECT COUNT(*) FROM anuncios 
                WHERE modelo IS NULL OR modelo = '' OR 
                      precio IS NULL OR precio <= 0 OR 
                      anio IS NULL OR anio < ?
            """, (Config.AÑO_MIN_VALIDO,))
            
            datos_invalidos = cur.fetchone()[0]
            if datos_invalidos > 0:
                problemas.append(f"{datos_invalidos} registros con datos inválidos")
            
            # Verificar precios fuera de rango
            cur.execute("""
                SELECT COUNT(*) FROM anuncios 
                WHERE precio < ? OR precio > ?
            """, (Config.PRECIO_MIN_VALIDO, Config.PRECIO_MAX_VALIDO))
            
            precios_invalidos = cur.fetchone()[0]
            if precios_invalidos > 0:
                problemas.append(f"{precios_invalidos} registros con precios fuera de rango")
            
            # Verificar links duplicados
            cur.execute("""
                SELECT COUNT(*) - COUNT(DISTINCT link) as duplicados 
                FROM anuncios WHERE link != ''
            """)
            
            links_duplicados = cur.fetchone()[0]
            if links_duplicados > 0:
                problemas.append(f"{links_duplicados} links duplicados")
            
            return {
                "problemas_encontrados": len(problemas),
                "detalle_problemas": problemas,
                "integridad_ok": len(problemas) == 0
            }
            
    except Exception as e:
        metricas.error("Error validando integridad de datos", e)
        return {
            "error": str(e),
            "integridad_ok": False
        }

# Función de utilidad para logging estructurado
def log_operacion(operacion: str, detalles: Dict[str, Any] = None):
    """Registrar operación con formato estructurado"""
    if Config.DEBUG:
        mensaje = f"🔍 {operacion}"
        if detalles:
            mensaje += f" - {detalles}"
        logger.info(mensaje)
    
    metricas.incrementar(f"operacion_{operacion.lower().replace(' ', '_')}")

# Función para resetear métricas (útil para testing)
def reset_metricas():
    """Resetear métricas de la sesión actual"""
    global metricas
    metricas = MetricasSession()
    logger.info("🔄 Métricas de sesión reseteadas")

# Función para obtener estado del sistema
def get_estado_sistema() -> Dict[str, Any]:
    """Obtener estado actual del sistema"""
    try:
        # Verificar conectividad de BD
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            bd_ok = cur.fetchone() is not None
        
        # Obtener estadísticas básicas
        stats = get_estadisticas_db()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "bd_conectada": bd_ok,
            "bd_path": Config.DB_PATH,
            "debug_mode": Config.DEBUG,
            "estadisticas": stats,
            "metricas_session": metricas.resumen(),
            "configuracion": {
                "score_min_db": Config.SCORE_MIN_DB,
                "score_min_telegram": Config.SCORE_MIN_TELEGRAM,
                "roi_minimo": Config.ROI_MINIMO,
                "precio_min_valido": Config.PRECIO_MIN_VALIDO,
                "precio_max_valido": Config.PRECIO_MAX_VALIDO
            }
        }
        
    except Exception as e:
        metricas.error("Error obteniendo estado del sistema", e)
        return {
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "bd_conectada": False
        }
