import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager
from correcciones import obtener_correccion

def escapar_multilinea(texto: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 0
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 1
DEPRECIACION_ANUAL = 0.08
MUESTRA_MINIMA_CONFIABLE = 5
MUESTRA_MINIMA_MEDIA = 2
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 1980
MAX_YEAR = CURRENT_YEAR + 1

# ----------------------------------------------------
# Configuración de pesos para calcular_score (MEJORADA)
WEIGHT_MODEL      = 120  # Aumentado para priorizar detecciones cerca del modelo
WEIGHT_TITLE      = 110  # Aumentado para dar más peso al título
WEIGHT_WINDOW     =  95
WEIGHT_GENERAL    =  70

PENALTY_INVALID   = -50  # Aumentado para ser más estricto
BONUS_VEHICULO    =  20  # Aumentado para premiar contexto vehicular
BONUS_PRECIO_HIGH =  15  # Aumentado para coherencia precio-año
PENALTY_FUTURO    = -60  # NUEVO: penalizar años futuros más severamente
BONUS_CONTEXTO_FUERTE = 25  # NUEVO: para contextos muy vehiculares
# ----------------------------------------------------

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
    "sin motor", "para partes", "no funciona", "accidentado", "partes disponibles", 
    "partes", "desarme", "solo piezas", "choque", "chocado"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Patrones precompilados para extraer año (MEJORADOS)
_PATTERN_YEAR_FULL = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b")  # Más restrictivo
_PATTERN_YEAR_SHORT = re.compile(r"['`´]?(\d{2})\b")
_PATTERN_YEAR_EMOJI = re.compile(r"([0-9️⃣]{4,8})")
_PATTERN_YEAR_SPECIAL = re.compile(r"\b(\d{1,2}[,.]\d{3})\b")

# MEJORADO: Patrones de contexto inválido más completos
_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro desde|publicado en|nacido en|creado en|registro|se unió a facebook en|perfil creado|calcomania|calcomanía|calcomanía:|calcomanía del:|visto por última vez|último acceso|graduado en|trabajó en|estudió en|empleado desde)\b.*?(19\d{2}|20\d{2})",
    flags=re.IGNORECASE
)

# NUEVO: Patrones de contexto vehicular fuerte
_PATTERN_VEHICULAR_FUERTE = re.compile(
    r"\b(?:modelo|año|del año|versión|m/|vehículo|carro|auto|motor|transmisión|automático|mecánico|standard|gasolina|diésel)\b",
    flags=re.IGNORECASE
)

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

# Crear patrones sin lookbehind variable (MEJORADO)
def create_model_year_pattern(sinonimos: Dict[str, List[str]]) -> re.Pattern:
    variantes = []
    for lista in sinonimos.values():
        variantes.extend(lista)

    # Escapar y ordenar por longitud (más largos primero para mejor matching)
    modelos_escapados = [re.escape(v) for v in sorted(variantes, key=len, reverse=True)]
    modelos_union = '|'.join(modelos_escapados)

    # MEJORADO: Patrones más precisos con word boundaries
    pattern = rf"""
        \b(?P<y1>(?:19[8-9]\d|20[0-2]\d|\d{{2}}))\s+(?:{modelos_union})\b  |  # año antes
        \b(?:{modelos_union})\s+(?P<y2>(?:19[8-9]\d|20[0-2]\d|\d{{2}}))\b     # año después
    """

    return re.compile(pattern, flags=re.IGNORECASE | re.VERBOSE)

# [Mantengo todos los sinónimos igual que antes...]
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
            "alto", "suzuki alto", "alto hatchbook", "alto 800", "alto k10",
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
            "swift", "suzuki swift", "swift hatchback", "swift 5dr", "swift 3dr",
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

_PATTERN_YEAR_AROUND_MODEL = create_model_year_pattern(sinonimos)

# MEJORADO: Patrón más específico para palabras clave
_PATTERN_YEAR_AROUND_KEYWORD = re.compile(
    r"(modelo|m/|versión|año|m\.|modelo:|año:|del|del:|md|md:)\s*[^\d]{0,5}([12]\d{3})", flags=re.IGNORECASE
)

_PATTERN_PRICE = re.compile(
    r"\b(?:q|\$)?\s*[\d.,]+(?:\s*quetzales?)?\b",
    flags=re.IGNORECASE
)

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
            # Crear tabla con estructura básica
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
            print("✅ Tabla anuncios creada con estructura básica")
        
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
        
        conn.commit()

def normalizar_formatos_ano(texto: str) -> str:
    # MEJORADO: Más específico para evitar falsos positivos
    # Convierte 2,009 o 2.009 → 2009 solo si parece un año
    texto = re.sub(r'\b(19|20)[,\.](\d{2})\b', r'\1\2', texto)
    return texto

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

def contiene_negativos(texto: str) -> bool:
    return any(p in texto.lower() for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

# MEJORADO: Validación de precio más estricta pero no excesiva
def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    # Rango base más estricto
    if precio < 2000 or precio > 600000:
        return False

    ref_info = get_precio_referencia(modelo, anio)
    precio_ref = ref_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    muestra = ref_info.get("muestra", 0)

    # Rangos más coherentes según la confianza de datos
    if "reparar" in texto.lower() or "repuesto" in texto.lower():
        margen_bajo = 0.1 * precio_ref
    else:
        if muestra >= MUESTRA_MINIMA_CONFIABLE:
            # Con datos confiables, ser más restrictivo pero permitir gangas
            margen_bajo = 0.25 * precio_ref  # 25% del precio de referencia
            margen_alto = 2.0 * precio_ref   # Máximo el doble
        else:
            # Sin datos confiables, usar rangos más amplios
            margen_bajo = 0.15 * precio_ref
            margen_alto = 2.5 * precio_ref
    
        return margen_bajo <= precio <= margen_alto

# MEJORADO: Extracción de precio más robusta
def limpiar_precio(texto: str) -> int:
    # Remover caracteres de moneda y normalizar
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    # Buscar números de 4-7 dígitos (precios típicos de carros)
    matches = re.findall(r"\b\d{4,7}\b", s)
    
    # CORREGIDO: Filtrar años para quedarse solo con precios
    candidatos = []
    for match in matches:
        num = int(match)
        # Si NO es un año válido, entonces podría ser un precio
        if not (MIN_YEAR <= num <= MAX_YEAR):
            candidatos.append(num)
    
    # Retornar el primer candidato válido como precio
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

# MEJORADO: Detección de modelo más robusta
def coincide_modelo(texto: str, modelo: str) -> bool:
    texto_l = unicodedata.normalize("NFKD", texto.lower())
    modelo_l = modelo.lower()

    variantes = sinonimos.get(modelo_l, []) + [modelo_l]
    texto_limpio = unicodedata.normalize("NFKD", texto_l).encode("ascii", "ignore").decode("ascii")
    
    # MEJORADO: Buscar coincidencias más precisas con word boundaries
    for variante in variantes:
        # Usar word boundary para evitar falsos positivos
        pattern = rf"\b{re.escape(variante)}\b"
        if re.search(pattern, texto_limpio, re.IGNORECASE):
            return True
    return False

# MEJORADO: Validación más estricta de candidatos a año
def es_candidato_año(raw: str) -> bool:
    orig = raw.strip()  
    
    # 1) Descartar decimales puros
    if re.fullmatch(r"\d+\.\d+", orig):
        return False

    # 2) Limpiar separadores pero mantener estructura
    raw_limpio = orig.strip("'\"").replace(",", "").replace(".", "")

    # 3) Validaciones de longitud y formato
    if len(raw_limpio) > 4 or len(raw_limpio) < 2:
        return False
    
    # 4) Descartar patrones obviamente no-años
    if raw_limpio.startswith("00"):
        return False

    # 5) Convertir y validar rango
    try:
        año = int(raw_limpio)
        # Para años de 2 dígitos, normalizar antes de validar
        if len(raw_limpio) == 2:
            año = 1900 + año if año > 50 else 2000 + año
        return MIN_YEAR <= año <= MAX_YEAR
    except ValueError:
        return False

# REEMPLAZA SOLO LA FUNCIÓN extraer_anio en tu código existente

def extraer_anio(texto, modelo=None, precio=None, debug=False):
    if not texto or not isinstance(texto, str):
        if debug:
            print("❌ Texto inválido o vacío")
        return None
    
    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)  
    texto_original = texto
    texto = texto.lower()

        # 🚫 Validación inicial: descartar si no hay contexto vehicular fuerte
    if not re.search(r'\b(modelo|año|versión|motor|vehículo|carro|auto|transmisión|automático|mecánico|gasolina|diésel)\b', texto):
        if debug:
            print("❌ No hay contexto vehicular suficiente para extraer año")
        return None

    # 1) PRIORIDAD MÁXIMA: Correcciones manuales
    correccion_manual = obtener_correccion(texto_original)
    if correccion_manual:
        if debug:
            print(f"✅ Corrección manual aplicada: {correccion_manual}")
        return correccion_manual

    def normalizar_año_corto(a):
        if a < 100:
            return 1900 + a if a > 50 else 2000 + a
        return a

    # CAMBIO CRÍTICO: Lista de candidatos con prioridades claras en lugar de diccionario acumulativo
    candidatos_prioritarios = []  # [(año, prioridad, fuente)]

    # 2) MÁXIMA PRIORIDAD: Patrones modelo-año específicos
    if modelo:
        modelo_variantes = sinonimos.get(modelo.lower(), [modelo.lower()])
        for variante in modelo_variantes:
            variante_escaped = re.escape(variante)
            
            # Patrón: "honda crv 2003" (año después del modelo)
            patron_despues = rf'\b{variante_escaped}\s+[^\d]*?(\d{{2,4}})\b'
            for match in re.finditer(patron_despues, texto):
                raw = match.group(1)
                if es_candidato_año(raw):
                    try:
                        año = int(raw)
                        año = normalizar_año_corto(año) if len(raw) == 2 else año
                        if MIN_YEAR <= año <= MAX_YEAR:
                            candidatos_prioritarios.append((año, 1000, f"modelo_después_{variante}"))
                            if debug:
                                print(f"🎯 ALTA PRIORIDAD: {año} después de {variante}")
                    except ValueError:
                        continue
            
            # Patrón: "2003 honda crv" (año antes del modelo)  
            patron_antes = rf'\b(\d{{2,4}})\s+[^\d]*?{variante_escaped}\b'
            for match in re.finditer(patron_antes, texto):
                raw = match.group(1)
                if es_candidato_año(raw):
                    try:
                        año = int(raw)
                        año = normalizar_año_corto(año) if len(raw) == 2 else año
                        if MIN_YEAR <= año <= MAX_YEAR:
                            candidatos_prioritarios.append((año, 1000, f"modelo_antes_{variante}"))
                            if debug:
                                print(f"🎯 ALTA PRIORIDAD: {año} antes de {variante}")
                    except ValueError:
                        continue

        # ⚡ Corte inmediato si hay un único año fuerte
        años_fuertes = [a for a, p, f in candidatos_prioritarios if p >= 1000]
        if len(set(años_fuertes)) == 1:
            if debug:
                print(f"✅ Corte inmediato: {años_fuertes[0]} (modelo+año claro)")
            return años_fuertes[0]


    # 3) ALTA PRIORIDAD: Palabras clave específicas
    patron_keywords = r'\b(?:modelo|m/|versión|año|del|año:|modelo:)\s*[^\d]{0,10}?(\d{2,4})\b'
    for match in re.finditer(patron_keywords, texto):
        raw = match.group(1)
        if es_candidato_año(raw):
            try:
                año = int(raw)
                año = normalizar_año_corto(año) if len(raw) == 2 else año
                if MIN_YEAR <= año <= MAX_YEAR:
                    candidatos_prioritarios.append((año, 900, "keyword"))
                    if debug:
                        print(f"🔑 KEYWORD: {año}")
            except ValueError:
                continue

    # 4) PRIORIDAD MEDIA: Primera línea/título
    primera_linea = texto.split('\n')[0] if '\n' in texto else texto[:150]
    for match in re.finditer(r'\b(\d{2,4})\b', primera_linea):
        raw = match.group(1)
        if es_candidato_año(raw):
            try:
                año = int(raw)
                año = normalizar_año_corto(año) if len(raw) == 2 else año
                if MIN_YEAR <= año <= MAX_YEAR:
                    # Verificar que no sea un precio obvio
                    contexto = primera_linea[max(0, match.start()-20):match.end()+20]
                    if not re.search(rf'[q$]\s*{re.escape(raw)}', contexto, re.IGNORECASE):
                        candidatos_prioritarios.append((año, 800, "titulo"))
                        if debug:
                            print(f"📄 TITULO: {año}")
            except ValueError:
                continue

    # 5) BAJA PRIORIDAD: Búsqueda general (solo si no hay candidatos de alta prioridad)
    if not any(prioridad >= 800 for _, prioridad, _ in candidatos_prioritarios):
        for match in re.finditer(r'\b(\d{2,4})\b', texto):
            raw = match.group(1)
            if es_candidato_año(raw):
                try:
                    año = int(raw)
                    año = normalizar_año_corto(año) if len(raw) == 2 else año
                    if MIN_YEAR <= año <= MAX_YEAR:
                        # Filtros estrictos para búsqueda general
                        contexto = texto[max(0, match.start()-30):match.end()+30]
                        
                        # Descartar contextos obviamente malos
                        if any(malo in contexto for malo in ['nacido', 'miembro desde', 'facebook', 'perfil']):
                            continue
                        
                        # Descartar si parece precio
                        if re.search(rf'[q$]\s*{re.escape(raw)}', contexto, re.IGNORECASE):
                            continue
                            
                        candidatos_prioritarios.append((año, 100, "general"))
                        if debug:
                            print(f"🔍 GENERAL: {año}")
                except ValueError:
                    continue

    # SELECCIÓN FINAL: Por prioridad más alta, luego por frecuencia
    if not candidatos_prioritarios:
        if debug:
            print("❌ No se encontraron candidatos")
        return None

    # Agrupar por año y encontrar la máxima prioridad para cada uno
    años_con_max_prioridad = {}
    for año, prioridad, fuente in candidatos_prioritarios:
        if año not in años_con_max_prioridad or prioridad > años_con_max_prioridad[año][0]:
            años_con_max_prioridad[año] = (prioridad, fuente)

    if debug:
        print("🎯 Candidatos finales:")
        for año, (prioridad, fuente) in sorted(años_con_max_prioridad.items(), key=lambda x: x[1][0], reverse=True):
            print(f"  - {año}: prioridad={prioridad}, fuente={fuente}")

    # Retornar el año con mayor prioridad
    año_final = max(años_con_max_prioridad.items(), key=lambda x: x[1][0])[0]
    
    if debug:
        print(f"✅ Año seleccionado: {año_final}")
    
    return año_final
    
    def calcular_score_mejorado(año: int, contexto: str, fuente: str, precio: Optional[int] = None) -> int:
        score = 0
        
        # Scores base mejorados
        if fuente == 'modelo':  
            score = WEIGHT_MODEL
        elif fuente == 'titulo': 
            score = WEIGHT_TITLE
        elif fuente == 'ventana': 
            score = WEIGHT_WINDOW
        else:                    
            score = WEIGHT_GENERAL

        # PENALIZACIONES MÁS SEVERAS
        # Contextos claramente no vehiculares
        contextos_malos = ['nacido', 'edad', 'años de edad', 'miembro desde', 'se unió', 
                          'facebook en', 'perfil creado', 'empleado desde', 'graduado']
        for mal in contextos_malos:
            if mal in contexto:
                score += PENALTY_INVALID  # -50
                break

        # Años futuros son muy sospechosos
        if año > CURRENT_YEAR:
            score += PENALTY_FUTURO  # -60

        # BONIFICACIONES MEJORADAS
        # Contexto vehicular fuerte
        if _PATTERN_VEHICULAR_FUERTE.search(contexto):
            score += BONUS_CONTEXTO_FUERTE  # +25

        # Palabras vehiculares adicionales
        vehiculares = ['vendo', 'automático', 'standard', 'gasolina', 'diésel', 
                      'kilometraje', 'papeles', 'traspaso']
        for veh in vehiculares:
            if veh in contexto:
                score += BONUS_VEHICULO  # +20
                break

        # Coherencia precio-año (si disponible)
        if precio and precio > 0:
            # Años más recientes con precios altos son coherentes
            if año >= 2010 and precio >= 15000:
                score += BONUS_PRECIO_HIGH
            # Años antiguos con precios bajos también
            elif año < 2000 and precio <= 25000:
                score += BONUS_PRECIO_HIGH

        return score

    def agregar_año_mejorado(raw, contexto, fuente='general'):
        if not es_candidato_año(raw):
            return
            
        try:
            año = int(raw.strip("'"))
            año = normalizar_año_corto(año) if año < 100 else año
            
            if MIN_YEAR <= año <= MAX_YEAR:
                score = calcular_score_mejorado(año, contexto, fuente, precio)
                candidatos[año] = max(candidatos.get(año, 0), score)
                if debug:
                    print(f"  Candidato {año}: score {score} (fuente: {fuente})")
        except ValueError:
            pass

    # 5) Búsqueda en título (alta prioridad)
    titulo = texto_limpio.split('\n')[0] if '\n' in texto_limpio else texto_limpio[:200]
    for match in re.finditer(r"(?:'|')?(\d{2,4})", titulo):
        raw = match.group(1)
        contexto_titulo = titulo[max(0, match.start() - 30):match.end() + 30]
        agregar_año_mejorado(raw, contexto_titulo, fuente='titulo')

    # 6) Búsqueda general (prioridad menor)
    for match in re.finditer(r"(?:'|')?(\d{2,4})", texto_limpio):
        raw = match.group(1)
        contexto_match = texto_limpio[max(0, match.start() - 40):match.end() + 40]
        agregar_año_mejorado(raw, contexto_match, fuente='general')

    # VALIDACIÓN FINAL ESTRICTA
    if not candidatos:
        if debug:
            print("❌ No se encontraron candidatos válidos")
        return None

    # Filtrar candidatos con score mínimo
    candidatos_validos = {año: score for año, score in candidatos.items() if score >= 60}
    
    if not candidatos_validos:
        if debug:
            print("❌ Ningún candidato supera el score mínimo de 60")
            print("Candidatos encontrados:")
            for a, s in sorted(candidatos.items(), key=lambda x: -x[1]):
                print(f"  - {a}: score {s}")
        return None

    if debug:
        print("🎯 Candidatos válidos (score >= 60):")
        for a, s in sorted(candidatos_validos.items(), key=lambda x: -x[1]):
            print(f"  - {a}: score {s}")

    # Retornar el año con mejor score
    año_final = max(candidatos_validos.items(), key=lambda x: x[1])[0]
    
    if debug:
        print(f"✅ Año seleccionado: {año_final}")
    
    return año_final

@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT precio FROM anuncios 
            WHERE modelo=? AND ABS(anio - ?) <= ? AND precio > 0
            ORDER BY precio
        """, (modelo, anio, tolerancia or TOLERANCIA_PRECIO_REF))
        precios = [row[0] for row in cur.fetchall()]
    
    if len(precios) >= MUESTRA_MINIMA_CONFIABLE:
        pf = filtrar_outliers(precios)
        med = statistics.median(pf)
        return {"precio": int(med), "confianza": "alta", "muestra": len(pf), "rango": f"{min(pf)}-{max(pf)}"}
    elif len(precios) >= MUESTRA_MINIMA_MEDIA:
        med = statistics.median(precios)
        return {"precio": int(med), "confianza": "media", "muestra": len(precios), "rango": f"{min(precios)}-{max(precios)}"}
    else:
        return {"precio": PRECIOS_POR_DEFECTO.get(modelo, 50000), "confianza": "baja", "muestra": 0, "rango": "default"}

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, anio: int, costo_extra: int = 2000) -> Dict[str, Any]:
    ref = get_precio_referencia(modelo, anio)
    años_ant = max(0, CURRENT_YEAR - anio)
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

# MEJORADO: Sistema de puntuación más coherente y estricto
@timeit
def puntuar_anuncio(anuncio: Dict[str, Any]) -> int:
    score = 50  # Score base neutro

    texto = anuncio.get("texto", "")
    modelo = anuncio.get("modelo", "")
    anio = anuncio.get("anio", CURRENT_YEAR)
    precio = anuncio.get("precio", 0)

    # PENALIZACIONES CRÍTICAS (pueden hacer el anuncio inválido)
    if contiene_negativos(texto):
        score -= 40  # Más severo para repuestos/desarme

    if es_extranjero(texto):
        score -= 30  # Más severo para ubicaciones extranjeras

    if not validar_precio_coherente(precio, modelo, anio):
        score -= 50  # Muy severo para precios incoherentes

    if anio > CURRENT_YEAR:
        score -= 60  # Años futuros son muy sospechosos

    # BONIFICACIONES POR CALIDAD
    # Contexto vehicular fuerte
    if _PATTERN_VEHICULAR_FUERTE.search(texto.lower()):
        score += 25

    # Palabras positivas vehiculares
    palabras_buenas = ['vehículo', 'automático', 'standard', 'papeles al día', 
                       'excelente estado', 'poco kilometraje', 'original']
    for palabra in palabras_buenas:
        if palabra in texto.lower():
            score += 15
            break

    # ROI y coherencia de mercado
    roi_info = get_precio_referencia(modelo, anio)
    precio_ref = roi_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    roi_valor = anuncio.get("roi", 0)
    confianza = roi_info.get("confianza", "baja")
    muestra = roi_info.get("muestra", 0)

    # Bonificaciones por oportunidad de inversión
    if roi_valor >= ROI_MINIMO * 2:  # ROI muy alto
        score += 30
    elif roi_valor >= ROI_MINIMO:
        score += 20

    # Ganga detectada (precio significativamente bajo)
    if precio < 0.7 * precio_ref:
        score += 25

    # Bonificación por datos confiables
    if confianza == "alta" and muestra >= MUESTRA_MINIMA_CONFIABLE:
        score += 15
    elif confianza == "media":
        score += 10

    # Penalización por falta de datos confiables con ROI bajo
    if confianza == "baja" and roi_valor < 5:
        score -= 20

    # Bonificación por descripción detallada
    if len(texto) > 300:
        score += 10
    elif len(texto) < 50:
        score -= 10

    return max(0, score)  # No permitir scores negativos

@timeit
def insertar_anuncio_db(link, modelo, anio, precio, km, roi, score, relevante=False,
                        confianza_precio=None, muestra_precio=None):
    conn = get_conn()
    cur = conn.cursor()
    
    # Verificar si existen las columnas adicionales
    cur.execute("PRAGMA table_info(anuncios)")
    columnas_existentes = {row[1] for row in cur.fetchall()}
    
    if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]):
        # Insertar con columnas adicionales
        cur.execute("""
        INSERT OR REPLACE INTO anuncios 
        (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio))
    else:
        # Insertar solo con columnas básicas
        cur.execute("""
        INSERT OR REPLACE INTO anuncios 
        (link, modelo, anio, precio, km, roi, score, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score))
    
    conn.commit()

def existe_en_db(link: str) -> bool:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
        return cur.fetchone() is not None

# MEJORADO: Verificación de cambios más granular
def anuncio_diferente(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Verifica si dos anuncios tienen diferencias significativas que justifiquen actualización"""
    campos_criticos = ["modelo", "anio", "precio"]  # Campos que requieren actualización obligatoria
    campos_secundarios = ["km", "roi", "score"]    # Campos que solo actualizan si hay cambios grandes
    
    # Si hay diferencias en campos críticos, siempre actualizar
    for campo in campos_criticos:
        if str(a.get(campo, "")) != str(b.get(campo, "")):
            return True
    
    # Para campos secundarios, solo actualizar si el cambio es significativo
    # ROI: diferencia mayor a 5%
    roi_a, roi_b = a.get("roi", 0), b.get("roi", 0)
    if abs(roi_a - roi_b) > 5:
        return True
        
    # Score: diferencia mayor a 10 puntos
    score_a, score_b = a.get("score", 0), b.get("score", 0)
    if abs(score_a - score_b) > 10:
        return True
        
    return False

@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
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
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT modelo, anio, precio, km, roi, score
            FROM anuncios
            WHERE link = ?
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

# FUNCIÓN PRINCIPAL MEJORADA con validación estricta
def analizar_mensaje(texto: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    """
    Analiza un mensaje y extrae información del vehículo con validación estricta.
    Retorna None si no se puede detectar año, modelo o precio de forma confiable.
    """
    if not texto or not isinstance(texto, str) or len(texto.strip()) < 10:
        if debug:
            print("❌ Texto inválido o demasiado corto")
        return None

    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)  

    # PASO 1: Detectar modelo (OBLIGATORIO)
    modelo = None
    for m in MODELOS_INTERES:
        if coincide_modelo(texto, m):
            modelo = m
            break
    
    if not modelo:
        if debug:
            print("❌ No se detectó ningún modelo válido")
        return None

    # PASO 2: Extraer precio (OBLIGATORIO)
    precio = limpiar_precio(texto)
    if not precio or precio < 2000:
        if debug:
            print(f"❌ Precio inválido: {precio}")
        return None

    # PASO 3: Extraer año (OBLIGATORIO) - Con validación estricta
    anio = extraer_anio(texto, modelo=modelo, precio=precio, debug=debug)
    if not anio:
        if debug:
            print("❌ No se pudo detectar el año de forma confiable")
        return None

    # PASO 4: Validación de coherencia precio-modelo-año
    if not validar_precio_coherente(precio, modelo, anio):
        if debug:
            print(f"❌ Precio {precio} no es coherente para {modelo} {anio}")
        return None

    # PASO 5: Calcular métricas
    roi_data = calcular_roi_real(modelo, precio, anio)
    
    anuncio_dict = {
        "texto": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio,
        "roi": roi_data.get("roi", 0)
    }
    
    score = puntuar_anuncio(anuncio_dict)

    # PASO 6: Extraer URL si existe
    url = ""
    for palabra in texto.split():
        if palabra.startswith("http"):
            url = limpiar_link(palabra)
            break

    # RESULTADO FINAL
    resultado = {
        "url": url,
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "roi": roi_data["roi"],
        "score": score,
        "relevante": score >= SCORE_MIN_TELEGRAM and roi_data["roi"] >= ROI_MINIMO,
        "km": "",
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data
    }

    if debug:
        print(f"✅ Análisis completado:")
        print(f"   Modelo: {modelo}")
        print(f"   Año: {anio}")
        print(f"   Precio: Q{precio:,}")
        print(f"   ROI: {roi_data['roi']:.1f}%")
        print(f"   Score: {score}")
        print(f"   Relevante: {'Sí' if resultado['relevante'] else 'No'}")

    return resultado
