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

PRECIOS_POR_DEFECTO = {
    "yaris": 45000, "civic": 65000, "corolla": 50000, "sentra": 42000,
    "rav4": 130000, "cr-v": 95000, "tucson": 70000, "kia picanto": 35000,
    "chevrolet spark": 30000, "nissan march": 37000, "suzuki alto": 26000,
    "suzuki swift": 40000, "hyundai accent": 41000, "mitsubishi mirage": 33000,
    "suzuki grand vitara": 52000, "hyundai i10": 34000, "kia rio": 40000,
    "toyota": 48000, "honda": 50000
}
MODELOS_INTERES = list(PRECIOS_POR_DEFECTO.keys())

PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "motor fundido", "no arranca", "no enciende", "papeles atrasados",
    "sin motor", "para partes", "no funciona", "accidentado"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Sinonimos para coincidencia de modelos
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

# Métricas de sesión
class MetricasSession:
    def __init__(self):
        self.inicio = datetime.now()
        self.contadores = defaultdict(int)
        self.errores = []
        self.warnings = []
    
    def incrementar(self, metrica: str, cantidad: int = 1):
        self.contadores[metrica] += cantidad
    
    def error(self, mensaje: str, excepcion: Optional[Exception] = None):
        self.errores.append({
            "timestamp": datetime.now(),
            "mensaje": mensaje,
            "excepcion": str(excepcion) if excepcion else None
        })
        logger.error(f"{mensaje}: {excepcion}" if excepcion else mensaje)
    
    def warning(self, mensaje: str):
        self.warnings.append({
            "timestamp": datetime.now(),
            "mensaje": mensaje
        })
        logger.warning(mensaje)
    
    def resumen(self) -> Dict[str, Any]:
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
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

# Inicialización de base de datos
os.makedirs(os.path.dirname(Config.DB_PATH), exist_ok=True)

def timeit(func):
    """Decorador para medir tiempo de ejecución"""
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
    """Context manager para conexiones de base de datos"""
    conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        metricas.error(f"Error en transacción de BD", e)
        raise
    finally:
        conn.close()

# Pool de conexiones simple
_conn: Optional[sqlite3.Connection] = None

def get_conn():
    """Obtener conexión reutilizable (legacy)"""
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
                # Crear tabla completa
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
                logger.info("✅ Tabla anuncios creada con estructura completa")
            else:
                # Agregar columnas faltantes
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
    """Validación centralizada con razón de rechazo"""
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
    """Insertar múltiples anuncios en una transacción"""
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
                        anuncio["score"], int(anuncio["relevante"]),
                        anuncio["confianza_precio"], anuncio["muestra_precio"]
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
    """Inserta o actualiza un anuncio y devuelve 'nuevo' o 'actualizado'"""
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
    """Eliminar anuncios muy antiguos para mantener DB limpia"""
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
    """Limpiar y normalizar links"""
    if not link:
        return ""
    try:
        link_limpio = ''.join(c for c in link.strip() if c.isascii() and c.isprintable())
        return link_limpio
    except Exception as e:
        metricas.warning(f"Error limpiando link: {e}")
        return ""

def contiene_negativos(texto: str) -> bool:
    """Verificar si el texto contiene palabras negativas"""
    try:
        texto_lower = texto.lower()
        return any(p in texto_lower for p in PALABRAS_NEGATIVAS)
    except Exception as e:
        metricas.warning(f"Error verificando palabras negativas: {e}")
        return False

def es_extranjero(texto: str) -> bool:
    """Verificar si el anuncio es de ubicación extranjera"""
    try:
        texto_lower = texto.lower()
        return any(p in texto_lower for p in LUGARES_EXTRANJEROS)
    except Exception as e:
        metricas.warning(f"Error verificando ubicación extranjera: {e}")
        return False

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    """Validar que el precio sea coherente con el modelo"""
    try:
        if precio < Config.PRECIO_MIN_VALIDO or precio > Config.PRECIO_MAX_VALIDO:
            return False
        
        precio_ref = PRECIOS_POR_DEFECTO.get(modelo, 50000)
        return 0.2 * precio_ref <= precio <= 2.5 * precio_ref
        
    except Exception as e:
        metricas.warning(f"Error validando precio coherente: {e}")
        return False

def limpiar_precio(texto: str) -> int:
    """Extraer precio del texto"""
    try:
        s = re.sub(r"[Qq\$\.,]", "", texto.lower())
        matches = re.findall(r"\b\d{3,7}\b", s)
        candidatos = [int(x) for x in matches if int(x) >= 3000 and (int(x) < 1990 or int(x) > AÑO_ACTUAL + 1)]
        
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
    """Filtrar outliers usando IQR"""
    if len(precios) < 4:
        return precios
        
    try:
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
        filtrados = [p for p in precios if lim_inf <= p <= lim_sup]
        
        if len(filtrados) >= 2:
            metricas.incrementar("outliers_filtrados", len(precios) - len(filtrados))
            return filtrados
        else:
            return precios
            
    except Exception as e:
        metricas.warning(f"Error filtrando outliers: {e}")
        return precios

def coincide_modelo(texto: str, modelo: str) -> bool:
    """Verificar si el texto coincide con un modelo específico"""
    try:
        texto_l = unicodedata.normalize("NFKD", texto.lower())
        modelo_l = modelo.lower()
        variantes = SINONIMOS_MODELO.get(modelo_l, []) + [modelo_l]
        texto_limpio = unicodedata.normalize("NFKD", texto_l).encode("ascii", "ignore").decode("ascii")
        
        coincide = any(v in texto_limpio for v in variantes)
        if coincide:
            metricas.incrementar("modelo_coincidido")
        
        return coincide
        
    except Exception as e:
        metricas.warning(f"Error verificando coincidencia de modelo: {e}")
        return False

def extraer_anio(texto: str, anio_actual: int = None) -> Optional[int]:
    """Extraer año del texto con múltiples patrones"""
    if anio_actual is None:
        anio_actual = AÑO_ACTUAL

    try:
        texto = texto.lower()
        
        # 1. Detectar años de 2 dígitos tipo "modelo 98"
        match_modelo = re.search(r"(modelo|año)\s?(\d{2})\b", texto)
        if match_modelo:
            anio = int(match_modelo.group(2))
            resultado = 1900 + anio if anio >= 90 else 2000 + anio
            if Config.AÑO_MIN_VALIDO <= resultado <= anio_actual:
                metricas.incrementar("anio_extraido_2digitos")
                return resultado

        # 2. Filtrar frases irrelevantes
        patrones_ignorar = [
            r"se unió a facebook en \d{4}",
            r"miembro desde \d{4}",
            r"en facebook desde \d{4}",
            r"perfil creado en \d{4}",
        ]
        for patron in patrones_ignorar:
            texto = re.sub(patron, '', texto)

        # 3. Extraer años de 4 dígitos válidos
        posibles = re.findall(r"\b(19\d{2}|20[0-3]\d)\b", texto)
        for anio in posibles:
            anio_int = int(anio)
            if Config.AÑO_MIN_VALIDO <= anio_int <= anio_actual:
                metricas.incrementar("anio_extraido_4digitos")
                return anio_int

        metricas.incrementar("anio_no_encontrado")
        return None
        
    except Exception as e:
        metricas.warning(f"Error extrayendo año: {e}")
        return None

def validar_coherencia_precio_año(precio: int, anio: int) -> bool:
    """Validar coherencia entre precio y año"""
    try:
        if anio >= 2020 and precio < 100_000:
            return False
        if anio >= 2016 and precio < 50_000:
            return False
        if anio >= 2010 and precio < 30_000:
            return False
        return True
        
    except Exception as e:
        metricas.warning(f"Error validando coherencia precio-año: {e}")
        return False

@timeit
@lru_cache(maxsize=128)
def get_precio_referencia_cached(modelo: str, anio: int, tolerancia: int) -> Tuple[int, str, int, str]:
    """Versión cacheada de get_precio_referencia"""
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
            pf = filtrar_outliers(precios)
            med = statistics.median(pf)
            return int(med), "alta", len(pf), f"{min(pf)}-{max(pf)}"
        elif len(precios) >= Config.MUESTRA_MINIMA_MEDIA:
            med = statistics.median(precios)
            return int(med), "media", len(precios), f"{min(precios)}-{max(precios)}"
        else:
            return PRECIOS_POR_DEFECTO.get(modelo, 50000), "baja", 0, "default"
            
    except Exception as e:
        metricas.error(f"Error obteniendo precio referencia para {modelo}", e)
        return PRECIOS_POR_DEFECTO.get(modelo, 50000), "baja", 0, "error"

@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """Obtener precio de referencia con cache"""
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
    """Calcular ROI real considerando depreciación"""
    try:
        ref = get_precio_referencia(modelo, anio)
        años_ant = max(0, AÑO_ACTUAL - anio)
        f_dep = (1 - Config.DEPRECIACION_ANUAL) ** años_ant
        p_dep = ref["precio"] * f_dep
        inv_total = precio_compra + costo_extra
        roi = ((p_dep - inv_total) / inv_total) * 100 if inv_total > 0 else 0.0
        
        metricas.incrementar("roi_calculado")
        
        return {
            "roi": round(roi, 1),
            "precio_referencia": ref["precio"],
            "precio_depreciado": int(p_dep),
            "confianza": ref["confianza"],
            "muestra": ref["muestra"],
            "inversion_total": inv_total,
            "años_antiguedad": años_ant
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

@timeit
def puntuar_anuncio(texto: str, roi_info: Optional[Dict] = None) -> int:
    """Puntuar anuncio basado en múltiples factores"""
    try:
        precio = limpiar_precio(texto)
        anio = extraer_anio(texto)
        modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
        
        if not (modelo and anio and precio):
            return 0
            
        if not validar_precio_coherente(precio, modelo, anio):
            return 0
            
        roi = roi_info["roi"] if roi_info else calcular_roi_real(modelo, precio, anio)["roi"]
        
        score = 4  # Score base
        
        # Bonificación por ROI
        if roi >= 25: score += 4
        elif roi >= 15: score += 3
        elif roi >= 10: score += 2
        elif roi >= 5: score += 1
        else: score -= 1
        
        # Bonificación por precio bajo
        if precio <= 25000: score += 2
        elif precio <= 35000: score += 1
        
        # Bonificación por descripción detallada
        if len(texto.split()) >= 8: score += 1
        
        score_final = max(0, min(score, 10))
        metricas.incrementar("anuncio_puntuado")
        
        return score_final
        
    except Exception as e:
        metricas.error(f"Error puntuando anuncio: {e}")
        return 0

@timeit
def insertar_anuncio_db(link, modelo, anio, precio, km, roi, score, relevante=False,
                        confianza_precio=None, muestra_precio=None):
    """Insertar anuncio en BD (función legacy)"""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}

        if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]):
            cur.execute("""
            INSERT OR REPLACE INTO anuncios 
            (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
            """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio))
        else:
            cur.execute("""
            INSERT OR REPLACE INTO anuncios 
            (link, modelo, anio, precio, km, roi, score, fecha_scrape)
            VALUES (?, ?, ?, ?, ?, ?, ?, DATE('now'))
            """, (link, modelo, anio, precio, km, roi, score))

        conn.commit()
        metricas.incrementar("anuncio_insertado_legacy")
        
    except Exception as e:
        metricas.error(f"Error insertando anuncio legacy {link}", e)
        raise

def existe_en_db(link: str) -> bool:
    """Verificar si un anuncio ya existe en la BD"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
            existe = cur.fetchone() is not None
            
            if existe:
                metricas.incrementar("anuncio_ya_existe")
            
            return existe
            
    except Exception as e:
        metricas.error(f"Error verificando existencia de {link}", e)
        return False

@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    """Obtener rendimiento de un modelo en los últimos días"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
                FROM anuncios WHERE modelo = ? AND fecha_scrape >= date('now', ?)
        """, (SCORE_MIN_DB, modelo, f"-{dias} days"))
        result = cur.fetchone()[0]
        return round(result or 0.0, 3)

@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

def get_estadisticas_db() -> Dict[str, Any]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total = cur.fetchone()[0]
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}
        if "confianza_precio" in columnas_existentes:
            cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'alta'")
            alta_conf = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'baja'")
            baja_conf = cur.fetchone()[0]
        else:
            alta_conf = 0
            baja_conf = total
        cur.execute("""
            SELECT modelo, COUNT(*) FROM anuncios 
            GROUP BY modelo ORDER BY COUNT(*) DESC
        """)
        por_modelo = dict(cur.fetchall())
        return {
            "total_anuncios": total,
            "alta_confianza": alta_conf,
            "baja_confianza": baja_conf,
            "porcentaje_defaults": round((baja_conf / total) * 100, 1) if total else 0,
            "por_modelo": por_modelo
        }

def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    if not (modelo and anio and precio):
        return None
    if not validar_precio_coherente(precio, modelo, anio):
        return None
    roi_data = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio(texto, roi_data)
    url = next((l for l in texto.split() if l.startswith("http")), "")
    return {
        "url": limpiar_link(url),
        "modelo": modelo,
        "año": anio,  # Cambié anio por año para mantener consistencia
        "precio": precio,
        "roi": roi_data["roi"],
        "score": score,
        "relevante": score >= SCORE_MIN_TELEGRAM and roi_data["roi"] >= ROI_MINIMO,
        "km": "",
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data
    }
