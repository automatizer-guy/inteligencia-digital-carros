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
TOLERANCIA_PRECIO_REF = 1
DEPRECIACION_ANUAL = 0.08
MUESTRA_MINIMA_CONFIABLE = 5
MUESTRA_MINIMA_MEDIA = 2
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 1980
MAX_YEAR = CURRENT_YEAR + 1

# ----------------------------------------------------
# Configuración de pesos para calcular_score
WEIGHT_MODEL      = 50
WEIGHT_TITLE      = 45
WEIGHT_WINDOW     = 40
WEIGHT_GENERAL    = 30

# === PENALTIES Y BONUSES BALANCEADOS ===
PENALTY_INVALID_LIGHT = -10    # Nuevo: penalty leve para contextos dudosos
PENALTY_INVALID_MEDIUM = -20   # Nuevo: penalty medio para contextos problemáticos
PENALTY_INVALID_HEAVY = -40    # Reducido de -30 -> -40, pero menos frecuente

BONUS_VEHICULO_LIGHT = 8       # Nuevo: bonus leve por palabras vehiculares
BONUS_VEHICULO_MEDIUM = 15     # Igual que antes, para contexto fuerte
BONUS_VEHICULO_HEAVY = 25      # Nuevo: bonus alto para contexto muy fuerte

BONUS_PRECIO_COHERENTE = 15    # Nuevo: bonus por precio coherente con año
BONUS_ROI_EXCELENTE = 20       # Nuevo: bonus por ROI excepcional (>30%)
BONUS_ROI_BUENO = 10           # Bonus por ROI bueno (10-30%)
# ----------------------------------------------------




# === THRESHOLDS MENOS RESTRICTIVOS ===
PRECIO_MIN_ABSOLUTO = 3000     # Reducido de 3000 -> 1000 (permite más gangas)
PRECIO_MAX_ABSOLUTO = 800000   # Aumentado de 500000 -> 800000
ROI_MINIMO = 8.0               # Reducido de 10.0 -> 8.0 (menos restrictivo)
SCORE_MIN_TELEGRAM = 4         # Reducido de 6 -> 4 (menos restrictivo)

# === RANGOS DE PRECIOS MÁS FLEXIBLES ===
MARGEN_BAJO_CONFIABLE = 0.20   # 20% por debajo (era 30%)
MARGEN_ALTO_CONFIABLE = 3.0    # 300% por encima (era 250%)
MARGEN_BAJO_INCIERTO = 0.15    # 15% por debajo para datos inciertos
MARGEN_ALTO_INCIERTO = 4.0     # 400% por encima para datos inciertos




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
_PATTERN_YEAR_EMOJI = re.compile(r"([0-9️⃣]{4,8})")
_PATTERN_YEAR_SPECIAL = re.compile(r"\b(\d{1,2}[,.]\d{3})\b")

# Crear patrones sin lookbehind variable
def create_model_year_pattern(sinonimos: Dict[str, List[str]]) -> re.Pattern:
    # Unir todas las variantes de todos los modelos
    variantes = []
    for lista in sinonimos.values():
        variantes.extend(lista)

    # Escapar todas las variantes para el regex
    modelos_escapados = [re.escape(v) for v in variantes]
    modelos_union = '|'.join(modelos_escapados)

    # Regex: año antes o después del modelo (formato 2 o 4 dígitos)
    pattern = rf"""
        \b(?P<y1>\d{{2,4}})\s+(?:{modelos_union})\b  |  # año antes
        \b(?:{modelos_union})\s+(?P<y2>\d{{2,4}})\b     # año después
    """

    return re.compile(pattern, flags=re.IGNORECASE | re.VERBOSE)


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



_PATTERN_YEAR_AROUND_MODEL = create_model_year_pattern(sinonimos)


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


def normalizar_formatos_ano(texto: str) -> str:
    # Convierte 2,009 o 2.009 → 2009
    texto = re.sub(r'\b(\d)[,\.](\d{3})\b', r'\1\2', texto)
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

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    """
    Wrapper para mantener compatibilidad con el código existente.
    """
    resultado = validar_precio_coherente_dict(precio, modelo, anio)
    return resultado['valido']


def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{3,7}\b", s)
    año_actual = datetime.now().year
    candidatos = [int(x) for x in matches if not (MIN_YEAR <= int(x) <= MAX_YEAR)]
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
    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)  
    texto_original = texto  # ✅ NUEVO: Guardar texto original
    texto = texto.lower()
    candidatos = {}

    correccion_manual = obtener_correccion(texto_original)
    if correccion_manual:
        if debug:
            print(f"✅ Corrección manual aplicada para: {texto_original[:50]} → {correccion_manual}")
        return correccion_manual

    def normalizar_año_corto(a):
        if a < 100:
            return 1900 + a if a > 50 else 2000 + a
        return a

    
    # 1) Quitar contextos no válidos (nacido, miembro desde, perfil creado…)
    texto = _PATTERN_INVALID_CTX.sub("", texto)

    # 0) Búsqueda prioritaria: año tras modelo o cerca de "año"/"modelo"
    for pat in (_PATTERN_YEAR_AROUND_MODEL, _PATTERN_YEAR_AROUND_KEYWORD):
        m = pat.search(texto)
        if not m:
            continue
    
        if pat == _PATTERN_YEAR_AROUND_MODEL:
            raw = m.groupdict().get("y1") or m.groupdict().get("y2")
        else:
            raw = m.group(2)
    
        if not raw:
            continue  # evita errores si raw está vacío
    
        try:
            año = int(raw)
            norm = normalizar_año_corto(año) if len(raw) == 2 else año
            if MIN_YEAR <= norm <= MAX_YEAR:
                return norm
        except ValueError:
            continue  # skip si raw no es convertible



    def puntuar_candidato_ano(anio: int, contexto: str, modelo: Optional[str] = None) -> int:
        score = 0
        if modelo and coincide_modelo(contexto, modelo):
            score += 40
        score += _score_contexto_vehicular_mejorado(contexto, [modelo] if modelo else [])
        if anio > datetime.now().year:
            score -= 40
        if re.search(_PATTERN_INVALID_CTX, contexto):
            score -= 30
        return score


    
    
def calcular_score(anuncio: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:
    """
    Función consolidada que reemplaza las múltiples funciones de scoring.
    Más balanceada y con scoring explicativo.
    """
    
    texto = anuncio.get("texto", "")
    modelo = anuncio.get("modelo", "")
    anio = anuncio.get("anio", datetime.now().year)
    precio = anuncio.get("precio", 0)
    roi = anuncio.get("roi", 0)
    
    score_detalle = {
        'base': 0,
        'precio': 0,
        'contexto': 0,
        'roi': 0,
        'coherencia': 0,
        'penalties': 0,
        'total': 0
    }
    
    # === 1. SCORE BASE SEGÚN CONTEXTO ===
    score_base = 0
    if modelo and modelo.lower() in texto.lower():
        score_base = WEIGHT_MODEL
        contexto_deteccion = "modelo_encontrado"
    elif any(palabra in texto.lower()[:100] for palabra in ['modelo', 'año', 'vendo']):
        score_base = WEIGHT_TITLE  
        contexto_deteccion = "titulo_vehicular"
    else:
        score_base = WEIGHT_GENERAL
        contexto_deteccion = "contexto_general"
    
    score_detalle['base'] = score_base
    
    # === 2. VALIDACIÓN Y SCORE DE PRECIO ===
    validacion_precio = validar_precio_coherente(precio, modelo, anio, debug)
    
    if validacion_precio['valido']:
        if validacion_precio['confianza'] == 'muy_alta':
            score_detalle['precio'] = BONUS_PRECIO_COHERENTE + 5
        elif validacion_precio['confianza'] == 'alta':
            score_detalle['precio'] = BONUS_PRECIO_COHERENTE
        elif validacion_precio['confianza'] == 'media':
            score_detalle['precio'] = BONUS_PRECIO_COHERENTE // 2
        else:
            score_detalle['precio'] = 0
    else:
        # Penalty por precio inválido, pero no tan severo
        if 'sospechosamente_bajo' in validacion_precio['razon']:
            score_detalle['precio'] = PENALTY_INVALID_HEAVY
        else:
            score_detalle['precio'] = PENALTY_INVALID_MEDIUM
    
    # === 3. SCORE DE CONTEXTO VEHICULAR ===
    score_contexto = calcular_score_contexto_vehicular(texto, modelo)
    score_detalle['contexto'] = score_contexto
    
    # === 4. SCORE DE ROI ===
    if roi >= 30:
        score_detalle['roi'] = BONUS_ROI_EXCELENTE
    elif roi >= ROI_MINIMO:
        score_detalle['roi'] = BONUS_ROI_BUENO
    elif roi >= 0:
        score_detalle['roi'] = 5  # Bonus mínimo por ROI positivo
    else:
        score_detalle['roi'] = PENALTY_INVALID_LIGHT  # Penalty leve por ROI negativo
    
    # === 5. PENALTIES POR CONTENIDO PROBLEMÁTICO ===
    penalties = 0
    
    # Palabras negativas con diferentes severidades
    palabras_muy_negativas = ["solo repuestos", "para repuestos", "motor fundido", "no arranca"]
    palabras_negativas_leves = ["repuesto", "papeles atrasados", "poco uso"]
    
    if any(p in texto.lower() for p in palabras_muy_negativas):
        penalties += PENALTY_INVALID_HEAVY
    elif any(p in texto.lower() for p in palabras_negativas_leves):
        penalties += PENALTY_INVALID_LIGHT
    
    # Lugares extranjeros
    if es_extranjero(texto):
        penalties += PENALTY_INVALID_MEDIUM
    
    score_detalle['penalties'] = penalties
    
    # === 6. SCORE TOTAL ===
    score_total = (score_detalle['base'] + score_detalle['precio'] + 
                  score_detalle['contexto'] + score_detalle['roi'] + 
                  score_detalle['penalties'])
    
    score_detalle['total'] = max(0, score_total)  # No permitir scores negativos
    
    resultado = {
        'score': score_detalle['total'],
        'score_detalle': score_detalle,
        'validacion_precio': validacion_precio,
        'contexto_deteccion': contexto_deteccion,
        'relevante': (score_detalle['total'] >= SCORE_MIN_TELEGRAM and 
                     roi >= ROI_MINIMO and validacion_precio['valido'])
    }
    
    if debug:
        print(f"🎯 Score consolidado: {score_detalle['total']}")
        print(f"   Desglose: {score_detalle}")
        print(f"   Relevante: {resultado['relevante']}")
    
    return resultado

def calcular_score_contexto_vehicular(texto: str, modelo: str = "") -> int:
    """
    Calcula score basado en contexto vehicular con diferentes niveles de bonus.
    """
    score = 0
    texto_lower = texto.lower()
    
    # === CONTEXTO MUY FUERTE (+25) ===
    patrones_muy_fuertes = [
        r'\b(modelo|año del|versión|motor|transmisión)\b',
        r'\b(vendo|se vende|en venta)\s+(mi|un|este|el)?\s*(carro|auto|vehículo)\b'
    ]
    
    # === CONTEXTO FUERTE (+15) ===  
    patrones_fuertes = [
        r'\b(toyota|honda|nissan|ford|chevrolet|hyundai|kia|mazda)\b',
        r'\b(sedan|hatchback|suv|pickup|automático|standard)\b',
        r'\b(kilometraje|km|millas|poco uso|bien cuidado)\b'
    ]
    
    # === CONTEXTO LEVE (+8) ===
    patrones_leves = [
        r'\b(usado|seminuevo|papeles|documentos|traspaso)\b',
        r'\b(llantas|aire acondicionado|excelente estado)\b'
    ]
    
    import re
    
    for patron in patrones_muy_fuertes:
        if re.search(patron, texto_lower):
            score += BONUS_VEHICULO_HEAVY
            break
    
    for patron in patrones_fuertes:
        if re.search(patron, texto_lower):
            score += BONUS_VEHICULO_MEDIUM
            break
    
    for patron in patrones_leves:
        if re.search(patron, texto_lower):
            score += BONUS_VEHICULO_LIGHT
            break
    
    return min(score, 40)  # Cap máximo para evitar scores excesivos


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


    mejores = [(a, puntuar_candidato_ano(a, texto, modelo)) for a in candidatos]
    anio_final = max(mejores, key=lambda x: x[1])[0]
    return anio_final


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
def puntuar_anuncio(anuncio: Dict[str, Any], debug: bool = False) -> int:
    """
    Función mejorada que reemplaza puntuar_anuncio() con el nuevo sistema.
    Mantiene compatibilidad retornando solo el score entero.
    """
    resultado = calcular_score(anuncio, debug)
    return resultado['score']




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
    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)  
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    if not (modelo and anio and precio):
        return None
    if not validar_precio_coherente(precio, modelo, anio):
        return None
    roi_data = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio({
        "texto": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio,
        "roi": roi_data.get("roi", 0)
    })  # ✅ Argumento único tipo dict

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
