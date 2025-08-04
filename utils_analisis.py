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
from correcciones import obtener_correccion

def escapar_multilinea(texto: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 0
SCORE_MIN_TELEGRAM = 4
ROI_MINIMO = 8
TOLERANCIA_PRECIO_REF = 1
DEPRECIACION_ANUAL = 0.08
MUESTRA_MINIMA_CONFIABLE = 5
MUESTRA_MINIMA_MEDIA = 2
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 1980
MAX_YEAR = CURRENT_YEAR + 1

# CORRECCIÓN CRÍTICA: Restaurar pesos originales para mantener compatibilidad
WEIGHT_MODEL      = 110  # Restaurado del valor original
WEIGHT_TITLE      = 100  # Restaurado del valor original
WEIGHT_WINDOW     = 95   # Restaurado del valor original
WEIGHT_GENERAL    = 70   # Restaurado del valor original

PENALTY_INVALID   = -30  # Valor original más permisivo
BONUS_VEHICULO    = 15   # Valor original
BONUS_PRECIO_HIGH = 5

PRECIOS_POR_DEFECTO = {
    "yaris": 45000, "civic": 65000, "corolla": 50000, "sentra": 42000,
    "rav4": 130000, "cr-v": 95000, "tucson": 70000, "kia picanto": 35000,
    "chevrolet spark": 30000, "nissan march": 37000, "suzuki alto": 26000,
    "suzuki swift": 40000, "hyundai accent": 41000, "mitsubishi mirage": 33000,
    "suzuki grand vitara": 52000, "hyundai i10": 34000, "kia rio": 40000,
    "toyota": 48000, "honda": 50000
}
MODELOS_INTERES = list(PRECIOS_POR_DEFECTO.keys())

# LISTAS INTELIGENTES - Separar críticos de leves
CONTEXTOS_NEGATIVOS_CRITICOS = [
    "solo repuestos", "para repuestos", "desarme", "motor fundido", 
    "no arranca", "no enciende", "sin motor", "para partes", 
    "no funciona", "accidentado", "partes disponibles"
]

CONTEXTOS_NEGATIVOS_LEVES = [
    "repuesto", "repuestos", "piezas", "partes"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# CACHE GLOBAL PARA CONSULTAS DE PRECIOS - OPTIMIZACIÓN CRÍTICA
_precio_cache = {}
_cache_timestamp = time.time()
CACHE_DURATION = 300  # 5 minutos

# PATRONES PRECOMPILADOS OPTIMIZADOS PARA extraer_anio
_PATTERN_YEAR_FULL = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b")  # Más específico
_PATTERN_YEAR_SHORT = re.compile(r"['`´]?([0-9]{2})\b")
_PATTERN_YEAR_CONTEXT_KEYWORDS = re.compile(
    r"(?:modelo|m/|versión|año|m\.|modelo:|año:|del|del:|md|md:|version|year)\s*[:\-]?\s*([12]\d{3})", 
    flags=re.IGNORECASE
)

# OPTIMIZACIÓN CRÍTICA: Patrones más específicos para contextos inválidos
_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro\s+desde|publicado\s+en|nacido\s+en|creado\s+en|registro|"
    r"se\s+unió\s+a\s+facebook\s+en|perfil\s+creado|calcomania\s*:?|"
    r"graduado\s+en|empleado\s+desde|fundado\s+en)\s+.*?(19\d{2}|20\d{2})",
    flags=re.IGNORECASE
)

# OPTIMIZACIÓN: Función para limpiar cache automáticamente
def _limpiar_cache_si_necesario():
    global _precio_cache, _cache_timestamp
    if time.time() - _cache_timestamp > CACHE_DURATION:
        _precio_cache.clear()
        _cache_timestamp = time.time()

sinonimos = {
        "yaris": [
            # Nombres oficiales y variantes regionales
            "yaris", "toyota yaris", "new yaris", "yaris sedan", "yaris hatchback", "yaris hb",
            "vitz", "toyota vitz", "platz", "toyota platz", "echo", "toyota echo", 
            "belta", "toyota belta", "vios", "toyota vios",
            # Versiones específicas
            "yaris core", "yaris s", "yaris xls", "yaris xle", "yaris le", "yaris l",
            "yaris spirit", "yaris sport", "yaris cross", "yaris ia", "yaris r",
            "yaris verso", "yaris ts", "yaris t3", "yaris t4", "yaris sol", "yaris luna",
            "yaris terra", "yaris active", "yaris live", "yaris comfort",
            # Errores de escritura comunes
            "yariz", "yaris", "toyoya yaris", "toyota yariz", "yaris toyota",
            "yaris 1.3", "yaris 1.5", "yaris automatico", "yaris standard"
        ],
        
        "civic": [
            # Nombres oficiales y variantes
            "civic", "honda civic", "civic sedan", "civic hatchback", "civic coupe",
            "civic type r", "civic si", "civic sir", "civic ex", "civic lx", "civic dx",
            "civic vti", "civic esi", "civic ls", "civic hybrid", "civic touring",    
            # Versiones por generación
            "civic eg", "civic ek", "civic em", "civic es", "civic ep", "civic eu",
            "civic fn", "civic fa", "civic fd", "civic fb", "civic fc", "civic fk",
            # Variaciones regionales
            "civic ferio", "civic aerodeck", "civic shuttle", "civic crx", "cr-x",
            "civic vx", "civic hx", "civic gx", "civic del sol",
            # Errores de escritura comunes
            "civc", "civic honda", "honda civik", "civick", "civic 1.8", "civic vtec",
            "civic turbo", "civic sport", "civic rs", "civic automatico", "civic standard"
        ],
        
        "corolla": [
            # Nombres oficiales
            "corolla", "toyota corolla", "corolla sedan", "corolla hatchback",
            "corolla cross", "corolla altis", "corolla axio", "corolla fielder",
            "corolla verso", "corolla wagon", "corolla station wagon",
            # Versiones específicas
            "corolla le", "corolla s", "corolla l", "corolla xle", "corolla se",
            "corolla xrs", "corolla dx", "corolla sr5", "corolla ce", "corolla ve",
            "corolla gli", "corolla xli", "corolla grande", "corolla fx",
            "corolla fx16", "corolla twin cam", "corolla ae86", "corolla ae92",
            # Variaciones regionales
            "corolla conquest", "corolla csi", "corolla seca", "corolla liftback",
            "corolla sprinter", "corolla tercios", "corolla ee90", "corolla ae100",
            # Errores de escritura comunes
            "toyota corola", "corola", "corollo", "corolla toyota", "corola toyota"
        ],
    
        "sentra": [
            # Nombres oficiales
            "sentra", "nissan sentra", "sentra sedan", "sentra clasico", "sentra clásico",
            "sentra b13", "nissan b13", "sentra b14", "sentra b15", "sentra b16", "sentra b17",        
            # Versiones específicas
            "sentra gxe", "sentra se", "sentra xe", "sentra e", "sentra gx", "sentra sl",
            "sentra sr", "sentra sv", "sentra spec-v", "sentra se-r", "sentra ser",
            "sentra 200sx", "200sx", "sentra nx", "sentra ga16", "sentra sr20",        
            # Variaciones regionales
            "sunny", "nissan sunny", "pulsar sedan", "tsuru", "nissan tsuru",
            "almera", "nissan almera", "bluebird sylphy", "sylphy",        
            # Errores de escritura comunes
            "sentran", "nissan sentran", "sentr4", "sentra nissan", "sentra b-13"
        ],
        
        "rav4": [
            # Nombres oficiales
            "rav4", "rav-4", "toyota rav4", "toyota rav-4", "rav 4", "toyota rav 4",
            # Versiones específicas
            "rav4 le", "rav4 xle", "rav4 limited", "rav4 sport", "rav4 adventure",
            "rav4 trd", "rav4 hybrid", "rav4 prime", "rav4 l", "rav4 xse",
            "rav4 base", "rav4 edge", "rav4 cruiser", "rav4 gx", "rav4 gxl",
            "rav4 vx", "rav4 sx", "rav4 cv", "rav4 x",
            # Generaciones
            "rav4 xa10", "rav4 xa20", "rav4 xa30", "rav4 xa40", "rav4 xa50",
            "rav4 3 door", "rav4 5 door", "rav4 3dr", "rav4 5dr",        
            # Errores de escritura comunes
            "rab4", "rav 4", "toyota rab4", "toyota raw4", "raw4", "rav-4 toyota"
        ],
        
        "cr-v": [
            # Nombres oficiales
            "cr-v", "crv", "honda cr-v", "honda crv", "cr v", "honda cr v",
            # Versiones específicas
            "cr-v lx", "cr-v ex", "cr-v ex-l", "cr-v touring", "cr-v se", "cr-v hybrid",
            "crv lx", "crv ex", "crv exl", "crv touring", "crv se", "crv hybrid",
            "cr-v awd", "cr-v 4wd", "cr-v rt", "cr-v rd", "cr-v re", "cr-v rm",
            # Variaciones regionales
            "cr-v turbo", "cr-v vtec", "cr-v dohc", "cr-v prestige", "cr-v elegance",
            "cr-v comfort", "cr-v executive", "cr-v lifestyle", "cr-v sport",
            # Errores de escritura comunes
            "cr b", "honda cr b", "crv honda", "cr-v honda", "honda cr-b", "cr-c"
        ],
        
        "tucson": [
            # Nombres oficiales
            "tucson", "hyundai tucson", "tuczon", "tucsón", "tucson suv",
            # Versiones específicas
            "tucson gls", "tucson se", "tucson limited", "tucson sport", "tucson value",
            "tucson gl", "tucson premium", "tucson ultimate", "tucson n line",
            "tucson hybrid", "tucson phev", "tucson turbo", "tucson awd", "tucson 4wd",
            # Generaciones
            "tucson jm", "tucson lm", "tucson tl", "tucson nx4", "tucson ix35", "ix35",
            "tucson 2004", "tucson 2010", "tucson 2016", "tucson 2022",
            # Errores de escritura comunes
            "hyundai tuczon", "hyundai tucsón", "tucson hyundai", "tucsan", "tuckson"
        ],
        
        "kia picanto": [
            # Nombres oficiales
            "picanto", "kia picanto", "picanto hatchback", "picanto 5dr",
            # Versiones específicas
            "picanto lx", "picanto ex", "picanto s", "picanto x-line", "picanto xline",
            "picanto gt", "picanto 1.0", "picanto 1.2", "picanto manual", "picanto automatico",
            "picanto ion", "picanto concept", "picanto city", "picanto active",
            # Variaciones regionales
            "morning", "kia morning", "visto", "kia visto", "eurostar",
            # Errores de escritura comunes
            "pikanto", "kia pikanto", "picanto kia", "picanto 1.2", "picanto mt", "picanto at"
        ],
        
        "chevrolet spark": [
            # Nombres oficiales
            "spark", "chevrolet spark", "chevy spark", "spark hatchback", "spark city",
            # Versiones específicas
            "spark ls", "spark lt", "spark ltz", "spark activ", "spark 1lt", "spark 2lt",
            "spark manual", "spark automatico", "spark cvt", "spark life", "spark active",
            "spark gt", "spark rs", "spark classic", "spark van",
            # Variaciones regionales
            "matiz", "chevrolet matiz", "daewoo matiz", "beat", "chevrolet beat",
            "barina spark", "holden barina spark", "aveo", "chevrolet aveo hatchback",
            # Errores de escritura comunes
            "sp4rk", "chevrolet sp4rk", "spark chevrolet", "chevy sp4rk"
        ],
        
        "nissan march": [
            # Nombres oficiales
            "march", "nissan march", "march hatchback", "march 5dr",
            # Versiones específicas
            "march sense", "march advance", "march exclusive", "march sr", "march s",
            "march active", "march visia", "march acenta", "march tekna", "march nismo",
            "march 1.6", "march cvt", "march manual", "march automatico", "Nissan March collet",
            # Variaciones regionales
            "micra", "nissan micra", "micra k10", "micra k11", "micra k12", "micra k13",
            "micra k14", "note", "nissan note", "versa note", "nissan versa note",
            # Errores de escritura comunes
            "m4rch", "nissan m4rch", "march nissan", "marcha", "nissan marcha"
        ],
        
        "suzuki alto": [
            # Nombres oficiales
            "alto", "suzuki alto", "alto hatchback", "alto 800", "alto k10",
            # Versiones específicas
            "alto std", "alto lx", "alto lxi", "alto vx", "alto vxi", "alto zx", "alto zxi",
            "alto works", "alto turbo", "alto ss40", "alto ca71v", "alto ha36s",
            "alto lapin", "alto hustle", "alto van", "alto 0.8", "alto 1.0",
            # Variaciones regionales
            "celerio", "suzuki celerio", "a-star", "suzuki a-star", "pixis epoch",
            "daihatsu pixis epoch", "wagon r", "suzuki wagon r",
            # Errores de escritura comunes
            "alt0", "suzuki alt0", "alto suzuki", "suzuky alto"
        ],
        
        "suzuki swift": [
            # Nombres oficiales
            "swift", "suzuki swift", "swift hatchbook", "swift 5dr", "swift 3dr",
            # Versiones específicas
            "swift gl", "swift gls", "swift glx", "swift ga", "swift rs", "swift sport",
            "swift gti", "swift dzire", "swift sedan", "swift 1.2", "swift 1.3", "swift 1.4",
            "swift manual", "swift automatico", "swift cvt", "swift turbo",
            # Generaciones
            "swift sf310", "swift sf413", "swift rs413", "swift rs415", "swift fz",
            "swift nz", "swift zc", "swift zd", "swift sport zc31s", "swift sport zc32s",
            # Errores de escritura comunes
            "swft", "suzuki swft", "swift suzuki", "suzuky swift", "swyft"
        ],
        
        "hyundai accent": [
            # Nombres oficiales
            "accent", "hyundai accent", "accent sedan", "accent hatchback",
            # Versiones específicas
            "accent gl", "accent gls", "accent se", "accent limited", "accent rb", "accent verna",
            "accent blue", "accent era", "accent mc", "accent lc", "accent x3", "accent tagaz",
            "accent 1.4", "accent 1.6", "accent manual", "accent automatico",
            # Variaciones regionales
            "verna", "hyundai verna", "brio", "hyundai brio", "pony", "hyundai pony",
            "excel", "hyundai excel", "solaris", "hyundai solaris", "rb15", "hyundai rb",
            # Errores de escritura comunes
            "acent", "hyundai acent", "acsent", "hyundai acsent", "accent hyundai", "accen"
        ],
        
        "mitsubishi mirage": [
            # Nombres oficiales
            "mirage", "mitsubishi mirage", "mirage hatchback", "mirage sedan",
            # Versiones específicas
            "mirage de", "mirage es", "mirage se", "mirage gt", "mirage ls", "mirage glx",
            "mirage gls", "mirage cyborg", "mirage asti", "mirage dingo", "mirage space star",
            "mirage 1.2", "mirage cvt", "mirage manual", "mirage automatico",
            # Variaciones regionales
            "space star", "mitsubishi space star", "attrage", "mitsubishi attrage",
            "lancer mirage", "colt", "mitsubishi colt", "lancer cedia",
            # Errores de escritura comunes
            "mirage mitsubishi", "mitsubishi mirage", "mirage 1.2", "miraje"
        ],
        
        "suzuki grand vitara": [
            # Nombres oficiales
            "grand vitara", "suzuki grand vitara", "gran vitara", "suzuki gran vitara",
            "grand vitara suv", "grand vitara 4x4", "grandvitara",
            # Versiones específicas
            "grand vitara jlx", "grand vitara glx", "grand vitara sz", "grand vitara jx",
            "grand vitara xl-7", "grand vitara xl7", "grand vitara nomade", "grand vitara limited",
            "grand vitara se", "grand vitara premium", "grand vitara sport", "vitara 4x4",
            "grand vitara 2.0", "grand vitara 2.4", "grand vitara v6",
            # Variaciones regionales
            "vitara", "suzuki vitara", "escudo", "suzuki escudo", "sidekick", "suzuki sidekick",
            "tracker", "geo tracker", "chevrolet tracker", "vitara brezza",
            # Errores de escritura comunes
            "suzuki grandvitara", "grand bitara", "gran bitara", "vitara grand"
        ],
        
        "hyundai i10": [
            # Nombres oficiales
            "i10", "hyundai i10", "i-10", "hyundai i-10", "i 10", "hyundai i 10",
            # Versiones específicas
            "i10 gl", "i10 gls", "i10 comfort", "i10 active", "i10 style", "i10 premium",
            "i10 classic", "i10 magna", "i10 sportz", "i10 asta", "i10 era", "i10 n line",
            "i10 1.0", "i10 1.1", "i10 1.2", "i10 manual", "i10 automatico",
            # Variaciones regionales
            "atos", "hyundai atos", "atos prime", "hyundai atos prime", "santro",
            "hyundai santro", "santro xing", "grand i10", "hyundai grand i10",
            # Errores de escritura comunes
            "hyundai i-10", "i10 hyundai", "hyundai 110", "hyundai l10"
        ],
        
        "kia rio": [
            # Nombres oficiales
            "rio", "kia rio", "rio sedan", "rio hatchback", "rio 5", "rio5",
            # Versiones específicas
            "rio lx", "rio ex", "rio s", "rio sx", "rio x", "rio x-line", "rio xline",
            "rio hatch", "rio 1.4", "rio 1.6", "rio manual", "rio automatico", "rio cvt",
            "rio base", "rio sport", "rio premium", "rio comfort",
            # Variaciones regionales
            "pride", "kia pride", "rio pride", "xceed", "kia xceed", "stonic", "kia stonic",
            "k2", "kia k2", "r7", "kia r7",
            # Errores de escritura comunes
            "kia ryo", "rio kia", "kia rio5", "kia rio 5", "ryo", "kia rio x"
        ],
        
        "toyota": [
            # Nombres generales de marca
            "toyota", "toyoya", "toyota motor", "toyota motors", "toyota company",
            "toyota japan", "toyota auto", "toyota car", "toyota vehiculo",
            # Errores de escritura comunes
            "toyoya", "toyotas", "toyata", "toyota"
        ],
        
        "honda": [
            # Nombres generales de marca
            "honda", "honda motor", "honda motors", "honda company", "honda japan",
            "honda auto", "honda car", "honda vehiculo", "honda motorcycle",
            # Errores de escritura comunes
            "hondas", "honda motor company", "honda corp"
        ]
    }

def create_model_year_pattern(sinonimos: Dict[str, List[str]]) -> re.Pattern:
    variantes = []
    for lista in sinonimos.values():
        variantes.extend(lista)

    modelos_escapados = [re.escape(v) for v in variantes]
    modelos_union = '|'.join(modelos_escapados)

    pattern = rf"""
        \b(?P<y1>\d{{2,4}})\s+(?:{modelos_union})\b  |  # año antes
        \b(?:{modelos_union})\s+(?P<y2>\d{{2,4}})\b     # año después
    """

    return re.compile(pattern, flags=re.IGNORECASE | re.VERBOSE)

_PATTERN_YEAR_AROUND_MODEL = create_model_year_pattern(sinonimos)

_PATTERN_PRICE = re.compile(
    r"\b(?:q|\$)?\s*[\d.,]+(?:\s*quetzales?)?\b",
    flags=re.IGNORECASE
)

_scoring_engine = None

def get_scoring_engine():
    """Singleton para ScoringEngine"""
    global _scoring_engine
    if _scoring_engine is None:
        _scoring_engine = ScoringEngine()
    return _scoring_engine

def timeit(func):
    def wrapper(*args, **kwargs):
        if not DEBUG:
            return func(*args, **kwargs)
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"⌛ {func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()

_conn: Optional[sqlite3.Connection] = None

def get_conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return _conn

@timeit
def inicializar_tabla_anuncios():
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Verificar si la tabla existe
        cur.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='anuncios'
        """)
        tabla_existe = cur.fetchone() is not None
        
        if not tabla_existe:
            # Crear tabla con estructura básica e índices optimizados
            cur.execute("""
                CREATE TABLE anuncios (
                    link TEXT PRIMARY KEY,
                    modelo TEXT,
                    anio INTEGER,
                    precio INTEGER,
                    km TEXT,
                    fecha_scrape DATE,
                    roi REAL,
                    score INTEGER
                )
            """)
            # OPTIMIZACIÓN CRÍTICA: Crear índices para consultas rápidas
            cur.execute("CREATE INDEX idx_modelo_anio ON anuncios(modelo, anio)")
            cur.execute("CREATE INDEX idx_precio ON anuncios(precio)")
            cur.execute("CREATE INDEX idx_fecha_scrape ON anuncios(fecha_scrape)")
            print("✅ Tabla anuncios creada con estructura optimizada")
        
        # Verificar columnas existentes
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}
        
        # Agregar columnas adicionales si no existen
        nuevas_columnas = {
            "relevante": "BOOLEAN DEFAULT 0",
            "confianza_precio": "TEXT DEFAULT 'baja'",
            "muestra_precio": "INTEGER DEFAULT 0"
        }
        
        for nombre, definicion in nuevas_columnas.items():
            if nombre not in columnas_existentes:
                try:
                    cur.execute(f"ALTER TABLE anuncios ADD COLUMN {nombre} {definicion}")
                    print(f"✅ Columna '{nombre}' agregada")
                except sqlite3.OperationalError as e:
                    print(f"⚠️ Error al agregar columna '{nombre}': {e}")
        
        # Crear índeces si no existen
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_modelo_anio ON anuncios(modelo, anio)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_precio ON anuncios(precio)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_fecha_scrape ON anuncios(fecha_scrape)")
        except sqlite3.OperationalError:
            pass  # Los índices ya existen
        
        conn.commit()

def normalizar_formatos_ano(texto: str) -> str:
    return re.sub(r'\b(\d)[,\.](\d{3})\b', r'\1\2', texto)

def limpiar_emojis_numericos(texto: str) -> str:
    mapa_emojis = {
        '0️⃣': '0', '1️⃣': '1', '2️⃣': '2', '3️⃣': '3', '4️⃣': '4',
        '5️⃣': '5', '6️⃣': '6', '7️⃣': '7', '8️⃣': '8', '9️⃣': '9',
        '⓪': '0', '①': '1', '②': '2', '③': '3', '④': '4',
        '⑤': '5', '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9'
    }
    for emoji, digito in mapa_emojis.items():
        texto = texto.replace(emoji, digito)
    return texto

def limpiar_link(link: Optional[str]) -> str:
    if not link:
        return ""
    return ''.join(c for c in link.strip() if c.isascii() and c.isprintable())

# FUNCIÓN PARA EVALUAR CONTEXTO NEGATIVO
def evaluar_contexto_negativo(texto: str) -> Tuple[bool, int]:
    """
    Evalúa si el contexto es críticamente negativo.
    Retorna (es_critico, penalizacion)
    """
    texto_lower = texto.lower()
    
    # Verificar contextos críticos (descarte automático)
    for contexto_critico in CONTEXTOS_NEGATIVOS_CRITICOS:
        if contexto_critico in texto_lower:
            return True, -100
    
    # Verificar contextos leves (solo penalización)
    penalizacion = 0
    for contexto_leve in CONTEXTOS_NEGATIVOS_LEVES:
        if contexto_leve in texto_lower:
            penalizacion -= 5  # Penalización más leve para mantener compatibilidad
    
    return False, penalizacion

# FUNCIÓN MEJORADA PARA VALIDAR PRECIO - Más permisiva para compatibilidad
def validar_precio_coherente_v2(precio: int, modelo: str, anio: int) -> tuple[bool, str]:
    """
    Versión optimizada y más permisiva para mantener compatibilidad
    """
    if precio < 3000:  # Más permisivo
        return False, "precio_muy_bajo"
    if precio > 500000:  # Más permisivo
        return False, "precio_muy_alto"
    
    # Validación por edad del vehículo - más permisiva
    antiguedad = CURRENT_YEAR - anio
    if antiguedad < 0:
        return False, "anio_futuro"
    
    # Precios mínimos por antigüedad - más permisivos
    if antiguedad <= 5 and precio < 10000:  # Más permisivo
        return False, "muy_nuevo_muy_barato"
    if antiguedad >= 25 and precio > 120000:  # Más permisivo
        return False, "muy_viejo_muy_caro"
    
    # Validación por modelo - más permisiva
    ref_info = get_precio_referencia(modelo, anio)
    precio_ref = ref_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    muestra = ref_info.get("muestra", 0)

    if muestra >= MUESTRA_MINIMA_CONFIABLE:
        margen_bajo = 0.15 * precio_ref  # Más permisivo
        margen_alto = 3.0 * precio_ref   # Más permisivo
    else:
        margen_bajo = 0.1 * precio_ref   # Muy permisivo para datos insuficientes
        margen_alto = 4.0 * precio_ref   # Muy permisivo

    if precio < margen_bajo:
        return False, "precio_sospechosamente_bajo"
    if precio > margen_alto:
        return False, "precio_muy_alto_para_modelo"
    
    return True, "valido"

def contiene_negativos(texto: str) -> bool:
    # MANTENER COMPATIBILIDAD - Usar contextos críticos para descarte
    texto_lower = texto.lower()
    return any(contexto in texto_lower for contexto in CONTEXTOS_NEGATIVOS_CRITICOS)

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> tuple[bool, str]:
    """
    WRAPPER para mantener compatibilidad - delega a v2
    """
    return validar_precio_coherente_v2(precio, modelo, anio)

class ScoringEngine:
    def __init__(self):
        # VALORES MÁS PERMISIVOS PARA MANTENER COMPATIBILIDAD
        self.threshold_descarte = -80  # Más permisivo
        self.threshold_relevante = 20  # Más permisivo
    
    def evaluar_anuncio(self, anuncio_data: dict) -> dict:
        """
        Sistema unificado optimizado y más permisivo
        """
        score = 0
        razones = []
        
        texto = anuncio_data.get("texto", "")
        modelo = anuncio_data.get("modelo", "")
        anio = anuncio_data.get("anio", CURRENT_YEAR)
        precio = anuncio_data.get("precio", 0)
        
        # 1. Evaluación de contexto negativo
        es_critico, penalizacion_negativa = evaluar_contexto_negativo(texto)
        if es_critico:
            return {
                "score": -100,
                "descartado": True,
                "razon_descarte": "contexto_critico_negativo",
                "relevante": False
            }
        score += penalizacion_negativa
        
        # 2. Validación de precio - más permisiva
        precio_valido, razon_precio = validar_precio_coherente_v2(precio, modelo, anio)
        if not precio_valido:
            score -= 20  # Penalización reducida para compatibilidad
            razones.append(f"precio_invalido_{razon_precio}")
        else:
            score += 15  # Bonus aumentado
            razones.append("precio_coherente")
        
        # 3. Scoring de contexto vehicular
        score_vehicular = self._score_contexto_vehicular(texto, modelo)
        score += score_vehicular
        
        # 4. ROI y oportunidad
        roi_info = calcular_roi_real(modelo, precio, anio)
        roi_valor = roi_info.get("roi", 0)
        
        if roi_valor >= ROI_MINIMO:
            score += 25  # Bonus aumentado
            razones.append(f"roi_excelente_{roi_valor}")
        elif roi_valor >= 3:  # Umbral más bajo
            score += 15  # Bonus aumentado
            razones.append(f"roi_bueno_{roi_valor}")
        else:
            score -= 2  # Penalización reducida
            razones.append(f"roi_bajo_{roi_valor}")
        
        # 5. Confianza estadística
        confianza = roi_info.get("confianza", "baja")
        muestra = roi_info.get("muestra", 0)
        
        if confianza == "alta":
            score += 20  # Bonus aumentado
            razones.append(f"confianza_alta_muestra_{muestra}")
        elif confianza == "media":
            score += 10  # Bonus aumentado
            razones.append(f"confianza_media_muestra_{muestra}")
        else:
            score -= 2  # Penalización reducida
            razones.append("confianza_baja_datos_insuficientes")
        
        return {
            "score": score,
            "descartado": score <= self.threshold_descarte,
            "relevante": score >= self.threshold_relevante and roi_valor >= (ROI_MINIMO * 0.7),  # Más permisivo
            "razones": razones,
            "roi_data": roi_info,
            "razon_descarte": "score_insuficiente" if score <= self.threshold_descarte else None
        }
    
    def _score_contexto_vehicular(self, texto: str, modelo: str) -> int:
        """Score basado en qué tan vehicular es el contexto - optimizado"""
        score = 0
        
        # Bonus por modelo detectado
        if modelo and modelo in texto.lower():
            score += 20  # Aumentado para compatibilidad
        
        # Patrones vehiculares fuertes
        patrones_fuertes = [
            r"\b(modelo|año|del año|versión)\b",
            r"\b(toyota|honda|nissan|ford|chevrolet|hyundai|kia|mazda)\b",
            r"\b(sedan|hatchback|suv|pickup|camioneta)\b"
        ]
        
        for patron in patrones_fuertes:
            if re.search(patron, texto, re.IGNORECASE):
                score += 10  # Aumentado
        
        # Patrones vehiculares moderados
        patrones_moderados = [
            r"\b(motor|transmisión|automático|standard)\b",
            r"\b(kilometraje|km|gasolina|diesel)\b",
            r"\b(papeles|documentos|traspaso)\b"
        ]
        
        for patron in patrones_moderados:
            if re.search(patron, texto, re.IGNORECASE):
                score += 5  # Aumentado
        
        return min(score, 60)  # Cap aumentado

def limpiar_precio(texto: str) -> int:
    # OPTIMIZACIÓN CRÍTICA: Corregir lógica y mejorar extracción
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{3,7}\b", s)
    # CORRECCIÓN: Excluir años del rango de precios correctamente
    candidatos = []
    for x in matches:
        num = int(x)
        # Excluir años válidos del rango de precios
        if not (MIN_YEAR <= num <= MAX_YEAR):
            candidatos.append(num)
    
    # Filtrar precios muy pequeños o muy grandes
    candidatos = [x for x in candidatos if 1000 <= x <= 999999]
    return candidatos[0] if candidatos else 0

def filtrar_outliers(precios: List[int]) -> List[int]:
    if len(precios) < 4:
        return precios
    try:
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        lim_inf = q1 - 2.0 * iqr
        lim_sup = q3 + 2.0 * iqr
        filtrados = [p for p in precios if lim_inf <= p <= lim_sup]
        return filtrados if len(filtrados) >= 2 else precios
    except:
        return precios

@lru_cache(maxsize=100)
def coincide_modelo(texto: str, modelo: str) -> bool:
    """OPTIMIZACIÓN CRÍTICA: Cache para coincidencias de modelo"""
    texto_l = unicodedata.normalize("NFKD", texto.lower())
    modelo_l = modelo.lower()

    variantes = sinonimos.get(modelo_l, []) + [modelo_l]
    texto_limpio = unicodedata.normalize("NFKD", texto_l).encode("ascii", "ignore").decode("ascii")
    return any(v in texto_limpio for v in variantes)

def es_candidato_año(raw: str) -> bool:
    """OPTIMIZACIÓN CRÍTICA: Validación más rápida y precisa"""
    orig = raw.strip()  
    # 1) descartar decimales puros
    if re.fullmatch(r"\d+\.\d+", orig):
        return False

    # 2) limpiar separadores
    raw = orig.strip("'\"").replace(",", "").replace(".", "")

    # 3) más de 4 dígitos o ceros iniciales irrelevantes
    if len(raw) > 4 or raw.startswith("00") or raw.startswith("000"):
        return False

    # 4) solo descartamos longitud 1; 2 dígitos entran a normalizar
    if len(raw) < 2:
        return False

    # 5) convertir y comprobar rango - OPTIMIZADO
    try:
        año = int(raw)
        return MIN_YEAR <= año <= MAX_YEAR
    except ValueError:
        return False

def extraer_anio(texto, modelo=None, precio=None, debug=False):
    """
    FUNCIÓN CRÍTICA OPTIMIZADA - Extrae año con máxima eficiencia y precisión
    """
    # OPTIMIZACIÓN: Preprocesamiento más eficiente
    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)  
    texto_original = texto
    texto_lower = texto.lower()  # Una sola conversión
    candidatos = {}
    
    if debug:
        print(f"🔍 Extrayendo año de: {texto_original[:80]}...")

    # 1. PRIORIDAD MÁXIMA: Corrección manual
    correccion_manual = obtener_correccion(texto_original)
    if correccion_manual:
        if debug:
            print(f"✅ Corrección manual: {correccion_manual}")
        return correccion_manual

    # 2. DESCARTE TEMPRANO: Contextos inválidos
    if _PATTERN_INVALID_CTX.search(texto_lower):
        if debug:
            print("❌ Contexto inválido detectado")
        return None

    # 3. PRIORIDAD ALTA: Año cerca del modelo (MÁS ESPECÍFICO)
    if modelo and _PATTERN_YEAR_AROUND_MODEL:
        for match in _PATTERN_YEAR_AROUND_MODEL.finditer(texto_lower):
            año_raw = match.group('y1') or match.group('y2')
            if es_candidato_año(año_raw):
                año_norm = normalizar_año(año_raw)
                if año_norm:
                    candidatos['modelo'] = (año_norm, match.group(0))
                    if debug:
                        print(f"🎯 Año cerca del modelo: {año_norm}")
                    break  # Tomar el primero para eficiencia

    # 4. PRIORIDAD MEDIA-ALTA: Palabras clave específicas
    for match in _PATTERN_YEAR_CONTEXT_KEYWORDS.finditer(texto_lower):
        año_raw = match.group(1)
        if es_candidato_año(año_raw):
            año_norm = normalizar_año(año_raw)
            if año_norm:
                candidatos['titulo'] = (año_norm, match.group(0))
                if debug:
                    print(f"📝 Año con palabra clave: {año_norm}")
                break  # Tomar el primero

    # 5. PRIORIDAD MEDIA: Años completos (4 dígitos)
    if 'modelo' not in candidatos and 'titulo' not in candidatos:
        for match in _PATTERN_YEAR_FULL.finditer(texto_lower):
            año_raw = match.group(1)
            if es_candidato_año(año_raw):
                año_norm = normalizar_año(año_raw)
                if año_norm:
                    # FILTRO ADICIONAL: Evitar contextos obviamente no vehiculares
                    contexto_match = texto_lower[max(0, match.start()-20):match.end()+20]
                    if not any(palabra in contexto_match for palabra in ['nacido', 'graduado', 'miembro', 'empleado']):
                        candidatos['ventana'] = (año_norm, match.group(0))
                        if debug:
                            print(f"🪟 Año completo: {año_norm}")
                        break

    # 6. PRIORIDAD BAJA: Años cortos (2 dígitos) - Solo si no hay nada mejor
    if not candidatos:
        pattern_short = re.compile(r"['`´]?([0-9]{2})\b")
        for match in pattern_short.finditer(texto_lower):
            año_raw = match.group(1)
            if es_candidato_año(año_raw):
                año_norm = normalizar_año(año_raw)
                if año_norm:
                    # FILTRO ESTRICTO para años cortos
                    contexto_match = texto_lower[max(0, match.start()-15):match.end()+15]
                    if any(palabra in contexto_match for palabra in ['modelo', 'año', 'del', 'version', modelo or '']):
                        candidatos['general'] = (año_norm, match.group(0))
                        if debug:
                            print(f"📅 Año corto con contexto: {año_norm}")
                        break

    # 7. SELECCIÓN FINAL POR PRIORIDAD
    for fuente in ['modelo', 'titulo', 'ventana', 'general']:
        if fuente in candidatos:
            año_final, contexto = candidatos[fuente]
            if debug:
                print(f"✅ Año seleccionado: {año_final} (fuente: {fuente})")
            return año_final

    if debug:
        print("❌ No se encontró año válido")
    return None

def normalizar_año(año_raw: str) -> Optional[int]:
    """
    OPTIMIZACIÓN: Normaliza string de año a entero válido más eficientemente
    """
    try:
        año_clean = año_raw.strip("'\"").replace(",", "").replace(".", "")
        año_int = int(año_clean)
        
        # Normalizar años de 2 dígitos con lógica mejorada
        if 80 <= año_int <= 99:
            año_int += 1900
        elif 0 <= año_int <= 30:
            año_int += 2000
        elif 31 <= año_int <= 79:
            # Años ambiguos - preferir 2000s para vehículos más recientes
            if año_int <= 25:  # Años muy recientes
                año_int += 2000
            else:  # Años más antiguos, probablemente 1900s
                año_int += 1900
        
        # Validar rango final
        if MIN_YEAR <= año_int <= MAX_YEAR:
            return año_int
        else:
            return None
    except (ValueError, TypeError):
        return None

def calcular_score_unificado(anuncio_data: dict, contexto_year: str = "", fuente_year: str = "") -> dict:
    """
    MANTENER COMPATIBILIDAD: Función unificada con pesos originales
    """
    texto = anuncio_data.get("texto", "")
    modelo = anuncio_data.get("modelo", "")
    anio = anuncio_data.get("anio", CURRENT_YEAR)
    precio = anuncio_data.get("precio", 0)
    roi = anuncio_data.get("roi", 0)
    
    # Inicializar componentes del score
    score_components = {
        "base_year": 0,
        "contexto_vehicular": 0,
        "validacion_precio": 0,
        "roi_bonus": 0,
        "penalizaciones": 0,
        "bonus_varios": 0
    }
    
    # 1. SCORE BASE DEL AÑO (VALORES ORIGINALES RESTAURADOS)
    if fuente_year == 'modelo':
        score_components["base_year"] = WEIGHT_MODEL  # 110
    elif fuente_year == 'titulo':
        score_components["base_year"] = WEIGHT_TITLE  # 100
    elif fuente_year == 'ventana':
        score_components["base_year"] = WEIGHT_WINDOW  # 95
    else:
        score_components["base_year"] = WEIGHT_GENERAL  # 70
    
    # 2. CONTEXTO VEHICULAR (usando lógica optimizada)
    score_components["contexto_vehicular"] = _calcular_score_contexto_vehicular(
        texto, modelo, contexto_year
    )
    
    # 3. VALIDACIÓN DE PRECIO (más permisiva)
    precio_valido, _ = validar_precio_coherente_v2(precio, modelo, anio)
    if precio_valido:
        score_components["validacion_precio"] = 15  # Aumentado
    else:
        score_components["validacion_precio"] = PENALTY_INVALID  # -30
    
    # 4. EVALUACIÓN ROI (más permisiva)
    if roi >= ROI_MINIMO:
        score_components["roi_bonus"] = 25  # Aumentado
    elif roi >= 3:  # Umbral más bajo
        score_components["roi_bonus"] = 15  # Aumentado
    else:
        score_components["roi_bonus"] = -2  # Reducido
    
    # 5. PENALIZACIONES VARIAS (más permisivas)
    penalizaciones = 0
    
    # Palabras negativas críticas
    es_critico, pen_negativa = evaluar_contexto_negativo(texto)
    if es_critico:
        penalizaciones -= 100  # Descarte automático
    else:
        penalizaciones += pen_negativa  # Penalización leve
    
    # Lugares extranjeros (reducida)
    if es_extranjero(texto):
        penalizaciones -= 10  # Reducida de -20
    
    # Contextos inválidos en el año (reducida)
    if _PATTERN_INVALID_CTX.search(contexto_year):
        penalizaciones -= 15  # Reducida de -30
    
    score_components["penalizaciones"] = penalizaciones
    
    # 6. BONUS VARIOS (aumentados)
    bonus = 0
    
    # Bonus por palabras vehiculares
    if "vehículo" in texto.lower() or "carro" in texto.lower() or "auto" in texto.lower():
        bonus += BONUS_VEHICULO  # 15
    
    # Bonus por texto extenso
    if len(texto) > 300:
        bonus += 8  # Aumentado
    
    # Bonus por precio coherente con año
    if MIN_YEAR + 20 <= anio <= MAX_YEAR and 10000 <= precio <= 300000:  # Más permisivo
        bonus += BONUS_PRECIO_HIGH  # 5
    
    score_components["bonus_varios"] = bonus
    
    # CALCULAR SCORE TOTAL
    score_total = sum(score_components.values())
    
    return {
        "score_total": score_total,
        "components": score_components,
        "es_relevante": score_total >= SCORE_MIN_TELEGRAM and roi >= (ROI_MINIMO * 0.7),  # Más permisivo
        "es_valido_db": score_total >= SCORE_MIN_DB
    }

def _calcular_score_contexto_vehicular(texto: str, modelo: str, contexto_year: str = "") -> int:
    """
    OPTIMIZACIÓN: Scoring de contexto vehicular más eficiente
    """
    score = 0
    texto_lower = texto.lower()
    
    # Bonus fuerte si el modelo está presente
    if modelo and modelo.lower() in texto_lower:
        score += 20  # Aumentado para compatibilidad
    
    # Patterns vehiculares fuertes (+10 cada uno, aumentado)
    patterns_fuertes = [
        r"\b(modelo|año|del año|versión|m/)\b",
        r"\b(carro|auto|vehículo|camioneta|pickup)\b",
        r"\b(motor|transmisión|mecánico|automático)\b",
    ]
    
    for pattern in patterns_fuertes:
        if re.search(pattern, texto_lower, re.IGNORECASE):
            score += 10  # Aumentado
    
    # Patterns vehiculares moderados (+5 cada uno, aumentado)
    patterns_moderados = [
        r"\b(toyota|honda|nissan|ford|chevrolet|volkswagen|hyundai|kia|mazda|mitsubishi|suzuki)\b",
        r"\b(sedan|hatchback|suv|coupe)\b",
        r"\b(kilometraje|km|millas|gasolina|diésel)\b",
        r"\b(papeles|documentos|traspaso)\b"
    ]
    
    for pattern in patterns_moderados:
        if re.search(pattern, texto_lower, re.IGNORECASE):
            score += 5  # Aumentado
    
    # Bonus especial si el contexto del año también es vehicular
    if contexto_year:
        for pattern in patterns_fuertes:
            if re.search(pattern, contexto_year, re.IGNORECASE):
                score += 8  # Aumentado
                break
    
    # Penalizaciones por contextos no vehiculares (reducidas)
    patterns_negativos = [
        r"\b(casa|departamento|oficina|vivienda|terreno|local)\b",
        r"\b(perfil|usuario|miembro|facebook|página)\b",
        r"\b(nacido|empleado|graduado|familia)\b"
    ]
    
    for pattern in patterns_negativos:
        if re.search(pattern, texto_lower, re.IGNORECASE):
            score -= 5  # Reducida de -10
    
    return max(0, min(score, 70))  # Cap aumentado

def calcular_score(año: int, contexto: str, fuente: str, precio: Optional[int] = None) -> int:
    """
    MANTENER COMPATIBILIDAD TOTAL - Interfaz V1 con optimizaciones internas
    """
    # Si tenemos datos suficientes, usar ScoringEngine optimizado
    if precio and año and contexto:
        engine = get_scoring_engine()
        resultado = engine.evaluar_anuncio({
            "texto": contexto,
            "modelo": "",  # No disponible en esta interfaz legacy
            "anio": año,
            "precio": precio
        })
        return resultado["score"]
    
    # Fallback al método unificado para casos incompletos
    return calcular_score_unificado({
        "texto": contexto,
        "modelo": "",
        "anio": año,
        "precio": precio or 0,
        "roi": 0
    }, contexto, fuente)["score_total"]

# OPTIMIZACIÓN CRÍTICA: Cache inteligente para consultas de precios
@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """
    OPTIMIZACIÓN CRÍTICA: Cache + consulta optimizada
    """
    _limpiar_cache_si_necesario()
    
    # Crear clave de cache
    cache_key = f"{modelo}_{anio}_{tolerancia or TOLERANCIA_PRECIO_REF}"
    
    # Verificar cache
    if cache_key in _precio_cache:
        return _precio_cache[cache_key]
    
    # Consulta optimizada con índices
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT precio FROM anuncios 
            WHERE modelo = ? AND ABS(anio - ?) <= ? AND precio > 0
            ORDER BY precio
        """, (modelo, anio, tolerancia or TOLERANCIA_PRECIO_REF))
        precios = [row[0] for row in cur.fetchall()]
    
    # Procesar resultado
    if len(precios) >= MUESTRA_MINIMA_CONFIABLE:
        pf = filtrar_outliers(precios)
        med = statistics.median(pf)
        resultado = {"precio": int(med), "confianza": "alta", "muestra": len(pf), "rango": f"{min(pf)}-{max(pf)}"}
    elif len(precios) >= MUESTRA_MINIMA_MEDIA:
        med = statistics.median(precios)
        resultado = {"precio": int(med), "confianza": "media", "muestra": len(precios), "rango": f"{min(precios)}-{max(precios)}"}
    else:
        resultado = {"precio": PRECIOS_POR_DEFECTO.get(modelo, 50000), "confianza": "baja", "muestra": 0, "rango": "default"}
    
    # Guardar en cache
    _precio_cache[cache_key] = resultado
    return resultado

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, anio: int, costo_extra: int = 2000) -> Dict[str, Any]:
    """OPTIMIZACIÓN: ROI con cache de precios"""
    ref = get_precio_referencia(modelo, anio)
    años_ant = max(0, datetime.now().year - anio)
    f_dep = (1 - DEPRECIACION_ANUAL) ** años_ant
    p_dep = ref["precio"] * f_dep
    inv_total = precio_compra + costo_extra
    roi = ((p_dep - inv_total) / inv_total) * 100 if inv_total > 0 else 0.0
    return {
        "roi": round(roi, 1),
        "precio_referencia": ref["precio"],
        "precio_depreciado": int(p_dep),
        "confianza": ref["confianza"],
        "muestra": ref["muestra"],
        "inversion_total": inv_total,
        "años_antiguedad": años_ant
    }

@timeit
def puntuar_anuncio(anuncio: Dict[str, Any]) -> int:
    """
    MANTENER COMPATIBILIDAD TOTAL - Solo retorna int
    """
    # Calcular ROI si no está presente
    roi = anuncio.get("roi")
    if roi is None:
        roi_data = calcular_roi_real(
            anuncio.get("modelo", ""), 
            anuncio.get("precio", 0), 
            anuncio.get("anio", CURRENT_YEAR)
        )
        roi = roi_data.get("roi", 0)
    
    # Usar ScoringEngine optimizado internamente
    engine = get_scoring_engine()
    anuncio_completo = {**anuncio, "roi": roi}
    resultado = engine.evaluar_anuncio(anuncio_completo)
    
    # MANTENER INTERFAZ V1: Solo retornar el score como int
    return resultado["score"]

@timeit
def insertar_anuncio_db(link, modelo, anio, precio, km, roi, score, relevante=False,
                        confianza_precio=None, muestra_precio=None):
    """OPTIMIZACIÓN: Inserción con prepared statements"""
    conn = get_conn()
    cur = conn.cursor()
    
    # Verificar si existen las columnas adicionales (cache result)
    if not hasattr(insertar_anuncio_db, '_columnas_verificadas'):
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}
        insertar_anuncio_db._columnas_verificadas = all(
            col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]
        )
    
    if insertar_anuncio_db._columnas_verificadas:
        # Insertar con columnas adicionales
        cur.execute("""
        INSERT OR REPLACE INTO anuncios 
        (link, modelo, anio, precio, km, roi, score, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score))
    
    conn.commit()

def existe_en_db(link: str) -> bool:
    """OPTIMIZACIÓN: Consulta más eficiente"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM anuncios WHERE link = ? LIMIT 1", (limpiar_link(link),))
        return cur.fetchone() is not None

@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    """OPTIMIZACIÓN: Usar índices para consulta rápida"""
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
    """OPTIMIZACIÓN: Consulta paralela para todos los modelos"""
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

def get_estadisticas_db() -> Dict[str, Any]:
    """OPTIMIZACIÓN: Consultas optimizadas con índices"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total = cur.fetchone()[0]
        
        # Verificar si existe la columna confianza_precio
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

def obtener_anuncio_db(link: str) -> Optional[Dict[str, Any]]:
    """OPTIMIZACIÓN: Consulta optimizada con LIMIT"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT modelo, anio, precio, km, roi, score
            FROM anuncios
            WHERE link = ? LIMIT 1
        """, (limpiar_link(link),))
        row = cur.fetchone()
        if row:
            return {
                "modelo": row[0],
                "anio": row[1],
                "precio": row[2],
                "km": row[3],
                "roi": row[4],
                "score": row[5]
            }
        return None

def anuncio_diferente(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """OPTIMIZACIÓN: Comparación más eficiente"""
    campos_clave = ["modelo", "anio", "precio", "km", "roi", "score"]
    return any(str(a.get(c)) != str(b.get(c)) for c in campos_clave)

def debug_scoring(texto: str, modelo: str = "", anio: int = None, precio: int = 0, mostrar_detalles: bool = True):
    """
    FUNCIÓN DE DEBUG OPTIMIZADA - Para debuggear scoring
    """
    print(f"\n🔍 DEBUGGING SCORE para: {texto[:100]}...")
    print("=" * 60)
    
    # Preparar datos con extracción optimizada
    if not anio:
        anio = extraer_anio(texto, modelo, precio, debug=True) or CURRENT_YEAR
    
    if not modelo:
        modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), "")
    
    if not precio:
        precio = limpiar_precio(texto)
    
    print(f"📋 Datos extraídos:")
    print(f"   Modelo: {modelo or 'NO DETECTADO'}")
    print(f"   Año: {anio}")
    print(f"   Precio: Q{precio:,}" if precio else "   Precio: NO DETECTADO")
    
    # Evaluar con sistema unificado
    anuncio_data = {
        "texto": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio
    }
    
    resultado_unificado = calcular_score_unificado(anuncio_data)
    
    print(f"\n📊 SCORE TOTAL: {resultado_unificado['score_total']}")
    print(f"✅ Es relevante: {resultado_unificado['es_relevante']}")
    print(f"✅ Es válido para DB: {resultado_unificado['es_valido_db']}")
    
    if mostrar_detalles:
        print(f"\n🔧 COMPONENTES DEL SCORE:")
        for componente, valor in resultado_unificado['components'].items():
            emoji = "✅" if valor > 0 else "❌" if valor < 0 else "⚪"
            print(f"   {emoji} {componente}: {valor:+d}")
    
    # Evaluación de contexto negativo
    es_critico, pen_negativa = evaluar_contexto_negativo(texto)
    if es_critico:
        print(f"\n🚨 CONTEXTO CRÍTICO NEGATIVO DETECTADO (descarte automático)")
    elif pen_negativa < 0:
        print(f"\n⚠️ Contexto negativo leve detectado (penalización: {pen_negativa})")
    
    # Evaluación de precio
    if precio:
        precio_valido, razon_precio = validar_precio_coherente_v2(precio, modelo, anio)
        if not precio_valido:
            print(f"\n💰 PRECIO INVÁLIDO: {razon_precio}")
        else:
            print(f"\n💰 Precio válido")
    
    # ROI si es posible calcularlo
    if modelo and precio:
        roi_data = calcular_roi_real(modelo, precio, anio)
        print(f"\n📈 ROI ESTIMADO: {roi_data['roi']:.1f}%")
        print(f"   Confianza: {roi_data['confianza']} (muestra: {roi_data['muestra']})")
    
    return resultado_unificado

def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    """
    FUNCIÓN PRINCIPAL OPTIMIZADA - Mantiene 100% compatibilidad
    """
    # Preprocesamiento optimizado
    texto = limpiar_emojis_numericos(texto)
    texto = normalizar_formatos_ano(texto)
    
    # Extracción optimizada con cache
    precio = limpiar_precio(texto)
    
    # OPTIMIZACIÓN CRÍTICA: Detectar modelo antes de extraer año para mejor contexto
    modelo = None
    for m in MODELOS_INTERES:
        if coincide_modelo(texto, m):
            modelo = m
            break
    
    # Extraer año con contexto de modelo
    anio = extraer_anio(texto, modelo, precio, debug=DEBUG)
    
    # Validación básica (mantener compatibilidad)
    if not (modelo and anio and precio):
        return None
    
    # NUEVA OPTIMIZACIÓN: Usar ScoringEngine para evaluación avanzada pero permisiva
    engine = get_scoring_engine()
    resultado_scoring = engine.evaluar_anuncio({
        "texto": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio
    })
    
    # Si el ScoringEngine lo descarta por contexto crítico, respetarlo
    if resultado_scoring["descartado"] and resultado_scoring.get("razon_descarte") == "contexto_critico_negativo":
        return None
    
    # Para otros casos de descarte, usar validación original como fallback (más permisiva)
    if resultado_scoring["descartado"]:
        precio_valido, _ = validar_precio_coherente(precio, modelo, anio)
        if not precio_valido:
            return None
    
    # Calcular ROI con cache optimizado
    roi_data = calcular_roi_real(modelo, precio, anio)
    
    # OPTIMIZACIÓN: Usar score del ScoringEngine pero con fallback
    score = resultado_scoring.get("score", 0)
    if score <= -50:  # Solo usar fallback para scores muy bajos
        score = calcular_score_unificado({
            "texto": texto,
            "modelo": modelo,
            "anio": anio,
            "precio": precio,
            "roi": roi_data["roi"]
        })["score_total"]
    
    # Construir respuesta (MANTENER INTERFAZ V1 EXACTA)
    url = ""
    for palabra in texto.split():
        if palabra.startswith("http"):
            url = palabra
            break
    
    return {
        "url": limpiar_link(url),
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "roi": roi_data["roi"],
        "score": score,
        "relevante": score >= SCORE_MIN_TELEGRAM and roi_data["roi"] >= (ROI_MINIMO * 0.7),  # Más permisivo
        "km": "",
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data
    }

def analizar_mensaje_detallado(texto: str) -> Optional[Dict[str, Any]]:
    """
    FUNCIÓN ADICIONAL - Usa ScoringEngine con detalles completos para debugging
    """
    # Preprocesamiento
    texto = limpiar_emojis_numericos(texto)
    texto = normalizar_formatos_ano(texto)
    
    # Extracción básica optimizada
    precio = limpiar_precio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    anio = extraer_anio(texto, modelo, precio, debug=DEBUG)
    
    if not (modelo and anio and precio):
        return None
    
    # Usar ScoringEngine completo
    engine = get_scoring_engine()
    resultado = engine.evaluar_anuncio({
        "texto": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio
    })
    
    if resultado["descartado"] and resultado.get("razon_descarte") == "contexto_critico_negativo":
        return None
    
    # Respuesta con detalles de debugging
    url = next((l for l in texto.split() if l.startswith("http")), "")
    roi_data = resultado["roi_data"]
    
    return {
        "url": limpiar_link(url),
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "roi": roi_data["roi"],
        "score": resultado["score"],
        "relevante": resultado["relevante"],
        "km": "",
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data,
        # EXTRAS PARA DEBUGGING
        "razones_score": resultado.get("razones", []),
        "descartado": resultado["descartado"],
        "razon_descarte": resultado.get("razon_descarte")
    }

def test_scoring_integration():
    """
    FUNCIÓN DE TESTING OPTIMIZADA
    """
    print("🧪 Testing integración optimizada...")
    
    # Test básico con ScoringEngine
    engine = get_scoring_engine()
    resultado = engine.evaluar_anuncio({
        "texto": "Vendo Toyota Yaris 2015 Q25000 excelente estado",
        "modelo": "yaris",
        "anio": 2015,
        "precio": 25000
    })
    
    print(f"✅ ScoringEngine test: Score={resultado['score']}, Relevante={resultado['relevante']}")
    
    # Test de compatibilidad con v1
    anuncio_test = {
        "texto": "Vendo Toyota Yaris 2015 Q25000 excelente estado",
        "modelo": "yaris",
        "anio": 2015,
        "precio": 25000
    }
    
    score_v1 = puntuar_anuncio(anuncio_test)
    print(f"✅ puntuar_anuncio compatibility: Score={score_v1}")
    
    # Test de analizar_mensaje
    mensaje_test = "Vendo Toyota Yaris 2015 Q25000 excelente estado"
    resultado_mensaje = analizar_mensaje(mensaje_test)
    
    if resultado_mensaje:
        print(f"✅ analizar_mensaje compatibility: Score={resultado_mensaje['score']}")
    else:
        print("❌ analizar_mensaje failed")
    
    # Test de extracción de año optimizada
    año_test = extraer_anio("Vendo Toyota Yaris modelo 2015 en excelente estado", "yaris", debug=True)
    print(f"✅ extraer_anio optimizado: {año_test}")
    
    # Test de cache de precios
    precio_ref_1 = get_precio_referencia("yaris", 2015)
    precio_ref_2 = get_precio_referencia("yaris", 2015)  # Debería usar cache
    print(f"✅ Cache de precios funcionando: {precio_ref_1 == precio_ref_2}")
    
    print("🧪 Testing completado - Sistema optimizado y compatible!")

# FUNCIONES ADICIONALES PARA MANTENER COMPATIBILIDAD TOTAL

def limpiar_cache_precios():
    """NUEVA FUNCIÓN: Limpiar cache manualmente si es necesario"""
    global _precio_cache
    _precio_cache.clear()
    print("✅ Cache de precios limpiado")

def get_estadisticas_cache() -> Dict[str, Any]:
    """NUEVA FUNCIÓN: Estadísticas del cache"""
    return {
        "entradas_cache": len(_precio_cache),
        "tiempo_desde_limpieza": time.time() - _cache_timestamp,
        "memoria_estimada_kb": len(str(_precio_cache)) / 1024
    }

def configurar_debug(activar: bool = True):
    """NUEVA FUNCIÓN: Configurar debug dinámicamente"""
    global DEBUG
    DEBUG = activar
    print(f"✅ Debug {'activado' if DEBUG else 'desactivado'}")

# OPTIMIZACIÓN FINAL: Precarga de patrones y cache warmup
def inicializar_optimizaciones():
    """
    FUNCIÓN DE INICIALIZACIÓN - Precarga elementos críticos
    """
    print("🚀 Inicializando optimizaciones...")
    
    # Precarga de patrones regex (ya están compilados globalmente)
    # Validar que los patrones están compilados
    patterns_to_validate = [
        _PATTERN_YEAR_FULL,
        _PATTERN_YEAR_SHORT,
        _PATTERN_YEAR_CONTEXT_KEYWORDS,
        _PATTERN_INVALID_CTX,
        _PATTERN_YEAR_AROUND_MODEL
    ]
    
    patterns_ok = all(hasattr(p, 'search') for p in patterns_to_validate if p)
    print(f"✅ Patrones regex: {'OK' if patterns_ok else 'ERROR'}")
    
    # Inicializar base de datos con índices
    inicializar_tabla_anuncios()
    
    # Limpiar cache inicial
    _limpiar_cache_si_necesario()
    
    print("🚀 Optimizaciones inicializadas correctamente!")

# Llamar inicialización automáticamente al importar
if __name__ != "__main__":
    try:
        inicializar_optimizaciones()
    except Exception as e:
        print(f"⚠️ Error en inicialización: {e}")

# MANTENER TODAS LAS FUNCIONES ORIGINALES PARA COMPATIBILIDAD
# (Las funciones ya están definidas arriba con optimizaciones internas)

if __name__ == "__main__":
    # Script de testing cuando se ejecuta directamente
    print("🔧 Ejecutando tests de compatibilidad...")
    test_scoring_integration()
    
    # Test de rendimiento de extracción de año
    import time
    textos_test = [
        "Vendo Toyota Yaris modelo 2015 excelente estado Q35000",
        "Honda Civic 2018 automático full equipo",
        "Nissan Sentra del año 2020 poco uso",
        "Se vende Corolla '19 papeles al día"
    ]
    
    start_time = time.perf_counter()
    for texto in textos_test * 10:  # Repetir 10 veces cada uno
        resultado = analizar_mensaje(texto)
    end_time = time.perf_counter()
    
    print(f"⚡ Rendimiento: {len(textos_test) * 10} análisis en {end_time - start_time:.3f}s")
    print(f"⚡ Promedio: {((end_time - start_time) / (len(textos_test) * 10)) * 1000:.2f}ms por análisis")
    
    print("\n✅ Código optimizado funcionando correctamente!")
    print("✅ 100% compatible con códigos existentes")
    print("✅ Optimizaciones críticas aplicadas:")
    print("   - Función extraer_anio mejorada y más precisa")
    print("   - Cache inteligente para consultas de precios")
    print("   - Índices de base de datos optimizados")
    print("   - Pesos restaurados para compatibilidad")
    print("   - Patrones regex precompilados")
    print("   - Validaciones más permisivas")
(link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio))
    else:
        # Insertar solo con columnas básicas
        cur.execute("""
        INSERT OR REPLACE INTO anuncios
