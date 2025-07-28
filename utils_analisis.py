import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager

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
# Configuración de pesos para calcular_score
WEIGHT_MODEL      = 110
WEIGHT_TITLE      = 100
WEIGHT_WINDOW     =  95
WEIGHT_GENERAL    =  70

PENALTY_INVALID   = -30    # contextos engañosos: nacido, edad, etc.
BONUS_VEHICULO    =  15    # presencia de palabras vehículo
BONUS_PRECIO_HIGH =  10    # bonus si precio encaja con año
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
    "partes", "desarme", "solo piezas"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Patrones precompilados para extraer año
_PATTERN_YEAR_FULL = re.compile(r"\b(19\d{2}|20\d{2})\b")
_PATTERN_YEAR_SHORT = re.compile(r"['`´]?(\d{2})\b")

# Crear patrones sin lookbehind variable
def create_model_pattern():
    modelos_escapados = [re.escape(m) for m in MODELOS_INTERES]
    # Usar captura de grupo en lugar de lookbehind
    pattern = rf"\b(?:{'|'.join(modelos_escapados)})\s+['`\u00b4]?(?P<y>\d{{2,4}})\b"
    return re.compile(pattern, flags=re.IGNORECASE)

_PATTERN_YEAR_AFTER_MODEL = create_model_pattern()

_PATTERN_YEAR_AROUND_KEYWORD = re.compile(
    r"(modelo|m/|versión|año|m.|modelo:|año:|del|del:|md|md:)[^\d]{0,5}([12]\d{3})", flags=re.IGNORECASE
)


_PATTERN_PRICE = re.compile(
    r"\b(?:q|\$)?\s*[\d.,]+(?:\s*quetzales?)?\b",
    flags=re.IGNORECASE
)
_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro desde|publicado en|nacido en|creado en|registro|Se unió a Facebook en|perfil creado|calcomania|calcomania:|calcomania del|calcomania del:)\b.*?(19\d{2}|20\d{2})",
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

def limpiar_link(link: Optional[str]) -> str:
    if not link:
        return ""
    return ''.join(c for c in link.strip() if c.isascii() and c.isprintable())

def contiene_negativos(texto: str) -> bool:
    return any(p in texto.lower() for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    if precio < 5000 or precio > 500000:
        return False

    ref_info = get_precio_referencia(modelo, anio)
    precio_ref = ref_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    muestra = ref_info.get("muestra", 0)

    # Si hay datos confiables, usar rango dinámico basado en precio_ref real
    if muestra >= MUESTRA_MINIMA_CONFIABLE:
        margen_bajo = 0.3 * precio_ref  # más permisivo para gangas
        margen_alto = 2.5 * precio_ref
    else:
        # Si no hay muestra confiable, usar rangos clásicos con precio por defecto
        margen_bajo = 0.2 * precio_ref
        margen_alto = 2.5 * precio_ref

    return margen_bajo <= precio <= margen_alto


def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{3,7}\b", s)
    año_actual = datetime.now().year
    candidatos = [int(x) for x in matches if int(x) < 1990 and int(x) > año_actual + 1]
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

def coincide_modelo(texto: str, modelo: str) -> bool:
    texto_l = unicodedata.normalize("NFKD", texto.lower())
    modelo_l = modelo.lower()
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






    
    variantes = sinonimos.get(modelo_l, []) + [modelo_l]
    texto_limpio = unicodedata.normalize("NFKD", texto_l).encode("ascii", "ignore").decode("ascii")
    return any(v in texto_limpio for v in variantes)

def es_candidato_año(raw: str) -> bool:
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

    # 5) convertir y comprobar rango
    try:
        año = int(raw)
        return MIN_YEAR <= año <= MAX_YEAR
    except ValueError:
        return False

def extraer_anio(texto, modelo=None, precio=None, debug=False):
    texto = texto.lower()
    candidatos = {}

    def normalizar_año_corto(a):
        if a < 100:
            return 1900 + a if a > 50 else 2000 + a
        return a

    
    # 1) Quitar contextos no válidos (nacido, miembro desde, perfil creado…)
    texto = _PATTERN_INVALID_CTX.sub("", texto)

    # 0) Búsqueda prioritaria: año tras modelo o cerca de "año"/"modelo"
    for pat in (_PATTERN_YEAR_AFTER_MODEL, _PATTERN_YEAR_AROUND_KEYWORD):
        m = pat.search(texto)
        if m:
            raw = m.group("y") if pat == _PATTERN_YEAR_AFTER_MODEL else m.group(2)
            año = int(raw)
            # Normalizar dos dígitos (ej. '19 → 2019')
            norm = normalizar_año_corto(año) if len(raw) == 2 else año
            # Verificar rango razonable
            if norm and MIN_YEAR <= norm <= MAX_YEAR:
                return norm

    def calcular_score(año: int, contexto: str, fuente: str, precio: Optional[int] = None) -> int:
        # Base
        if fuente == 'modelo':  score = WEIGHT_MODEL
        elif fuente == 'titulo': score = WEIGHT_TITLE
        elif fuente == 'ventana': score = WEIGHT_WINDOW
        else:                    score = WEIGHT_GENERAL
    
        # Penalizar contextos "engañosos"
        for mal in ('nacido', 'edad', 'años', 'miembro desde', 'se unió', 'Facebook en'):
            if mal in contexto:
                score += PENALTY_INVALID
                break
    
        # Bonus si habla de carro/motor/etc.
        for veh in ('modelo', 'año', 'motor', 'caja', 'carro',
                    'vehículo', 'vendo', 'automático', 'standard'):
            if veh in contexto:
                score += BONUS_VEHICULO
                break
            if re.search(r"(modelo|gxe|lx|le|gt|clásico)[^\n]{0,15}\b\d{2}\b", contexto):
                score += 20  # Bonus por patrón fuerte de año corto en contexto vehicular

    
        # Ajuste por precio
        if precio is not None:
            if MIN_YEAR + 25 <= año <= MAX_YEAR and 1500 <= precio <= 80000:
                score += BONUS_PRECIO_HIGH
            elif MIN_YEAR <= año < MIN_YEAR + 25 and precio < 30000:
                score += BONUS_PRECIO_HIGH
    
        return score

    def agregar_año(raw, contexto, fuente=''):
        try:
            año = int(raw.strip("'"))
            año = normalizar_año_corto(año) if año < 100 else año
            if año and MIN_YEAR <= año <= MAX_YEAR:
                candidatos[año] = max(candidatos.get(año, 0), calcular_score(año, contexto, fuente, precio))
        except:
            pass

    # 1. Búsqueda alrededor del modelo
    if modelo:
        idx = texto.find(modelo.lower())
        if idx != -1:
            ventana = texto[max(0, idx - 30): idx + len(modelo) + 30]
            años_modelo = re.findall(r"(?:'|')?(\d{2,4})", ventana)
            for raw in años_modelo:
                if es_candidato_año(raw):
                    agregar_año(raw, ventana, fuente='modelo')

    # 2. Búsqueda en título
    titulo = texto.split('\n')[0]
    años_titulo = re.findall(r"(?:'|')?(\d{2,4})", titulo)
    for raw in años_titulo:
        if es_candidato_año(raw):
            agregar_año(raw, titulo, fuente='titulo')

    # 3. General en todo el texto
    for match in re.finditer(r"(?:'|')?(\d{2,4})", texto):
        raw = match.group(1)
        contexto = texto[max(0, match.start() - 20):match.end() + 20]
        if es_candidato_año(raw):
            agregar_año(raw, contexto, fuente='texto')

    if not candidatos:
        if debug:
            print("❌ No se encontró ningún año válido.")
        return None

    if debug:
        print("🎯 Candidatos detectados:")
        for a, s in sorted(candidatos.items(), key=lambda x: -x[1]):
            print(f"  - {a}: score {s}")
            
    if not candidatos or max(candidatos.values()) < 40:
        if debug: print("❌ Todos los años tienen score insuficiente o dudoso.")
        return None


    return max(candidatos.items(), key=lambda x: x[1])[0]

def _remover_precios_del_texto_mejorado(texto: str) -> str:
    """
    Versión mejorada que remueve patrones de precios del texto más agresivamente.
    """
    # Patrones de precios más completos y específicos
    patrones_precio = [
        r"\bq\s*[\d,.\s]+\b",  # Q 14,000 o Q14000
        r"\$\s*[\d,.\s]+\b",   # $14,000
        r"\b\d{1,3}(?:[,.]\d{3})+\b",  # 14,000 o 14.000
        r"\bprecio\s*[:\-]?\s*[\d,.\s]+\b",  # precio: 14000
        r"\bvalor\s*[:\-]?\s*[\d,.\s]+\b",   # valor 14000
        r"\bcuesta\s*[\d,.\s]+\b",           # cuesta 14000
        r"\b[\d,.\s]+\s*quetzales?\b",       # 14000 quetzales
        r"\b[\d,.\s]+\s*mil\b",              # 14 mil
        r"\bnegociable\s*[\d,.\s]*\b",       # negociable 16000
        r"\bespecial[,\s]*no\s*negociable\b", # precio especial, no negociable
        
        # Patrones específicos para casos problemáticos
        r"\b(precio|valor)\s*[:\-]?\s*q?\s*\d{1,2}[,.]\d{3}\b",  # precio Q16,000
        r"\bq\d{2}[,.]\d{3}\b",  # Q16,000 directo
        r"\b\d{2}[,.]\d{3}\s*(quetzales?|efectivo|negociable)\b",  # 16,000 quetzales
        
        # Patrones para precios en formato completo
        r"\b\d{4,6}\s*(quetzales?|efectivo|negociable|final)\b",  # 15000 quetzales
        r"\b(Q|q)\s*\d{4,6}\b",  # Q15000
    ]
    
    texto_limpio = texto
    for patron in patrones_precio:
        texto_limpio = re.sub(patron, " ", texto_limpio, flags=re.IGNORECASE)
    
    # Limpiar espacios múltiples
    texto_limpio = re.sub(r'\s+', ' ', texto_limpio).strip()
    
    return texto_limpio

def _score_contexto_vehicular_mejorado(texto: str, modelos_detectados: List[str] = None) -> int:
    """
    Calcula un score mejorado de qué tan probable es que el contexto sea vehicular.
    """
    if modelos_detectados is None:
        modelos_detectados = []
    
    puntuacion = 0
    
    # BONUS MUY FUERTE: Si hay modelos de vehículos detectados cerca
    if modelos_detectados:
        for modelo in modelos_detectados:
            if modelo and modelo in texto:
                puntuacion += 10  # Bonus muy alto
    
    # PALABRAS VEHICULARES MUY FUERTES (+5 cada una)
    vehiculares_muy_fuertes = [
        r"\b(modelo|año|del año|versión|m/)\b",
        r"\b(carro|auto|vehículo|camioneta|pickup)\b",
        r"\b(motor|transmisión|mecánico|automático)\b",
    ]
    
    # PALABRAS VEHICULARES FUERTES (+3 cada una)
    vehiculares_fuertes = [
        r"\b(toyota|honda|nissan|ford|chevrolet|volkswagen|hyundai|kia|mazda|mitsubishi|suzuki)\b",
        r"\b(sedan|hatchback|suv|coupe)\b",
        r"\b(kilometraje|km|millas|gasolina|diésel)\b"
    ]
    
    # PALABRAS VEHICULARES MODERADAS (+1 cada una)
    vehiculares_moderadas = [
        r"\b(usado|seminuevo|equipado|papeles|documentos|traspaso)\b",
        r"\b(llantas|frenos|batería|aceite|aire acondicionado)\b",
        r"\b(bien cuidado|excelente estado|poco uso)\b"
    ]
    
    # PALABRAS NEGATIVAS (-5 cada una)
    penalizaciones_fuertes = [
        r"\b(casa|departamento|oficina|vivienda|terreno|local)\b",
        r"\b(perfil|usuario|miembro|facebook|página)\b",
        r"\b(teléfono|celular|contacto|whatsapp|email)\b"
    ]
    
    # PALABRAS NEGATIVAS MODERADAS (-2 cada una)
    penalizaciones_moderadas = [
        r"\b(nacido|empleado|graduado|familia|matrimonio)\b",
        r"\b(publicado|creado|actualizado|visto)\b"
    ]
    
    for patron in vehiculares_muy_fuertes:
        puntuacion += 5 * len(re.findall(patron, texto, re.IGNORECASE))
    
    for patron in vehiculares_fuertes:
        puntuacion += 3 * len(re.findall(patron, texto, re.IGNORECASE))
    
    for patron in vehiculares_moderadas:
        puntuacion += 1 * len(re.findall(patron, texto, re.IGNORECASE))
    
    for patron in penalizaciones_fuertes:
        puntuacion -= 5 * len(re.findall(patron, texto, re.IGNORECASE))
        
    for patron in penalizaciones_moderadas:
        puntuacion -= 2 * len(re.findall(patron, texto, re.IGNORECASE))
    
    return max(0, puntuacion)

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
    score = 0

    texto = anuncio.get("texto", "")
    modelo = anuncio.get("modelo", "")
    anio = anuncio.get("anio", CURRENT_YEAR)
    precio = anuncio.get("precio", 0)

    # Penalización si contiene palabras negativas
    if contiene_negativos(texto):
        score -= 3

    # Bonus si incluye palabras positivas como "vehículo"
    if "vehículo" in texto.lower():
        score += BONUS_VEHICULO

    # Penalización si parece extranjero
    if es_extranjero(texto):
        score -= 2

    # Validación de precio (sin return anticipado)
    if not validar_precio_coherente(precio, modelo, anio):
        score += PENALTY_INVALID  # -50

    # ROI y referencia del modelo-año
    roi_info = get_precio_referencia(modelo, anio)
    precio_ref = roi_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    roi_valor = anuncio.get("roi", roi_info.get("roi", 0))
    confianza = roi_info.get("confianza", "baja")
    muestra = roi_info.get("muestra", 0)

    # Ganga detectada (precio muy por debajo del mercado)
    if precio < 0.8 * precio_ref:
        score += 1

    # ROI fuerte
    if roi_valor >= ROI_MINIMO:
        score += 2

    # Bonus por confianza estadística alta
    if confianza == "alta" and muestra >= MUESTRA_MINIMA_CONFIABLE:
        score += 1

    # Penalización por ROI débil con poca muestra
    if confianza == "baja" and muestra < MUESTRA_MINIMA_CONFIABLE and roi_valor < 5:
        score -= 1

    # Bonus si el texto es extenso e informativo
    if len(texto) > 300:
        score += 1

    return score



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

def anuncio_diferente(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    campos_clave = ["modelo", "anio", "precio", "km", "roi", "score"]
    return any(str(a.get(c)) != str(b.get(c)) for c in campos_clave)

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
        "url": limpiar_link(url),  # Cambié link por url para mantener consistencia
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
