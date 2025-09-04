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
# NUEVA: Umbral para asignación inteligente de años
MUESTRA_MINIMA_ASIGNACION_AÑO = 30
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 1980
MAX_YEAR = CURRENT_YEAR + 1

# Configuración de pesos para calcular_score
WEIGHT_MODEL      = 120
WEIGHT_TITLE      = 110
WEIGHT_WINDOW     =  95
WEIGHT_GENERAL    =  70

PENALTY_INVALID   = -50
BONUS_VEHICULO    =  20
BONUS_PRECIO_HIGH =  15
PENALTY_FUTURO    = -60
BONUS_CONTEXTO_FUERTE = 25

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
    "partes", "desarme", "solo piezas", "choque", "chocado", "suspensión"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Patrones precompilados
_PATTERN_YEAR_FULL = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b")
_PATTERN_YEAR_SHORT = re.compile(r"['`´]?(\d{2})\b")
_PATTERN_YEAR_EMOJI = re.compile(r"([0-9️⃣]{4,8})")
_PATTERN_YEAR_SPECIAL = re.compile(r"\b(\d{1,2}[,.]\d{3})\b")

_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro desde|publicado en|nacido en|creado en|registro|se unió a facebook en|perfil creado|calcomania|calcomanía|calcomanía:|calcomanía del:|visto por última vez|último acceso|graduado en|trabajó en|estudió en|empleado desde)\b.*?(19\d{2}|20\d{2})",
    flags=re.IGNORECASE
)

_PATTERN_VEHICULAR_FUERTE = re.compile(
    r"\b(?:modelo|año|del año|versión|m/|vehículo|carro|auto|motor|transmisión|automático|mecánico|standard|gasolina|diésel)\b",
    flags=re.IGNORECASE
)

_PATTERN_PRICE = re.compile(
    r"\b(?:q|\$)?\s*[\d.,]+(?:\s*quetzales?)?\b",
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

# Sinónimos completos (mantener igual que antes)
sinonimos = {
    "yaris": [
        "yaris", "toyota yaris", "new yaris", "yaris sedan", "yaris hatchback", "yaris hb",
        "vitz", "toyota vitz", "platz", "toyota platz", "echo", "toyota echo", 
        "belta", "toyota belta", "vios", "toyota vios",
        "yaris core", "yaris s", "yaris xls", "yaris xle", "yaris le", "yaris l",
        "yaris spirit", "yaris sport", "yaris cross", "yaris ia", "yaris r",
        "yaris verso", "yaris ts", "yaris t3", "yaris t4", "yaris sol", "yaris luna",
        "yaris terra", "yaris active", "yaris live", "yaris comfort",
        "yariz", "yaris", "toyoya yaris", "toyota yariz", "yaris toyota",
        "yaris 1.3", "yaris 1.5", "yaris automatico", "yaris standard"
    ],
    "civic": [
        "civic", "honda civic", "civic sedan", "civic hatchback", "civic coupe",
        "civic type r", "civic si", "civic sir", "civic ex", "civic lx", "civic dx",
        "civic vti", "civic esi", "civic ls", "civic hybrid", "civic touring",    
        "civic eg", "civic ek", "civic em", "civic es", "civic ep", "civic eu",
        "civic fn", "civic fa", "civic fd", "civic fb", "civic fc", "civic fk",
        "civic ferio", "civic aerodeck", "civic shuttle", "civic crx", "cr-x",
        "civic vx", "civic hx", "civic gx", "civic del sol",
        "civc", "civic honda", "honda civik", "civick", "civic 1.8", "civic vtec",
        "civic turbo", "civic sport", "civic rs", "civic automatico", "civic standard"
    ],
    "corolla": [
        "corolla", "toyota corolla", "corolla sedan", "corolla hatchback",
        "corolla cross", "corolla altis", "corolla axio", "corolla fielder",
        "corolla verso", "corolla wagon", "corolla station wagon",
        "corolla le", "corolla s", "corolla l", "corolla xle", "corolla se",
        "corolla xrs", "corolla dx", "corolla sr5", "corolla ce", "corolla ve",
        "corolla gli", "corolla xli", "corolla grande", "corolla fx",
        "corolla fx16", "corolla twin cam", "corolla ae86", "corolla ae92",
        "corolla conquest", "corolla csi", "corolla seca", "corolla liftback",
        "corolla sprinter", "corolla tercios", "corolla ee90", "corolla ae100",
        "toyota corola", "corola", "corollo", "corolla toyota", "corola toyota"
    ],
    "sentra": [
        "sentra", "nissan sentra", "sentra sedan", "sentra clasico", "sentra clásico",
        "sentra b13", "nissan b13", "sentra b14", "sentra b15", "sentra b16", "sentra b17",        
        "sentra gxe", "sentra se", "sentra xe", "sentra e", "sentra gx", "sentra sl",
        "sentra sr", "sentra sv", "sentra spec-v", "sentra se-r", "sentra ser",
        "sentra 200sx", "200sx", "sentra nx", "sentra ga16", "sentra sr20",        
        "sunny", "nissan sunny", "pulsar sedan", "tsuru", "nissan tsuru",
        "almera", "nissan almera", "bluebird sylphy", "sylphy",        
        "sentran", "nissan sentran", "sentr4", "sentra nissan", "sentra b-13"
    ],
    "rav4": [
        "rav4", "rav-4", "toyota rav4", "toyota rav-4", "rav 4", "toyota rav 4",
        "rav4 le", "rav4 xle", "rav4 limited", "rav4 sport", "rav4 adventure",
        "rav4 trd", "rav4 hybrid", "rav4 prime", "rav4 l", "rav4 xse",
        "rav4 base", "rav4 edge", "rav4 cruiser", "rav4 gx", "rav4 gxl",
        "rav4 vx", "rav4 sx", "rav4 cv", "rav4 x",
        "rav4 xa10", "rav4 xa20", "rav4 xa30", "rav4 xa40", "rav4 xa50",
        "rav4 3 door", "rav4 5 door", "rav4 3dr", "rav4 5dr",        
        "rab4", "rav 4", "toyota rab4", "toyota raw4", "raw4", "rav-4 toyota"
    ],
    "cr-v": [
        "cr-v", "crv", "honda cr-v", "honda crv", "cr v", "honda cr v",
        "cr-v lx", "cr-v ex", "cr-v ex-l", "cr-v touring", "cr-v se", "cr-v hybrid",
        "crv lx", "crv ex", "crv exl", "crv touring", "crv se", "crv hybrid",
        "cr-v awd", "cr-v 4wd", "cr-v rt", "cr-v rd", "cr-v re", "cr-v rm",
        "cr-v turbo", "cr-v vtec", "cr-v dohc", "cr-v prestige", "cr-v elegance",
        "cr-v comfort", "cr-v executive", "cr-v lifestyle", "cr-v sport",
        "cr b", "honda cr b", "crv honda", "cr-v honda", "honda cr-b", "cr-c"
    ],
    "tucson": [
        "tucson", "hyundai tucson", "tuczon", "tucsón", "tucson suv",
        "tucson gls", "tucson se", "tucson limited", "tucson sport", "tucson value",
        "tucson gl", "tucson premium", "tucson ultimate", "tucson n line",
        "tucson hybrid", "tucson phev", "tucson turbo", "tucson awd", "tucson 4wd",
        "tucson jm", "tucson lm", "tucson tl", "tucson nx4", "tucson ix35", "ix35",
        "tucson 2004", "tucson 2010", "tucson 2016", "tucson 2022",
        "hyundai tuczon", "hyundai tucsón", "tucson hyundai", "tucsan", "tuckson"
    ],
    "kia picanto": [
        "picanto", "kia picanto", "picanto hatchback", "picanto 5dr",
        "picanto lx", "picanto ex", "picanto s", "picanto x-line", "picanto xline",
        "picanto gt", "picanto 1.0", "picanto 1.2", "picanto manual", "picanto automatico",
        "picanto ion", "picanto concept", "picanto city", "picanto active",
        "morning", "kia morning", "visto", "kia visto", "eurostar",
        "pikanto", "kia pikanto", "picanto kia", "picanto 1.2", "picanto mt", "picanto at"
    ],
    "chevrolet spark": [
        "spark", "chevrolet spark", "chevy spark", "spark hatchback", "spark city",
        "spark ls", "spark lt", "spark ltz", "spark activ", "spark 1lt", "spark 2lt",
        "spark manual", "spark automatico", "spark cvt", "spark life", "spark active",
        "spark gt", "spark rs", "spark classic", "spark van",
        "matiz", "chevrolet matiz", "daewoo matiz", "beat", "chevrolet beat",
        "barina spark", "holden barina spark", "aveo", "chevrolet aveo hatchback",
        "sp4rk", "chevrolet sp4rk", "spark chevrolet", "chevy sp4rk"
    ],
    "nissan march": [
        "march", "nissan march", "march hatchback", "march 5dr",
        "march sense", "march advance", "march exclusive", "march sr", "march s",
        "march active", "march visia", "march acenta", "march tekna", "march nismo",
        "march 1.6", "march cvt", "march manual", "march automatico", "Nissan March collet",
        "micra", "nissan micra", "micra k10", "micra k11", "micra k12", "micra k13",
        "micra k14", "note", "nissan note", "versa note", "nissan versa note",
        "m4rch", "nissan m4rch", "march nissan", "marcha", "nissan marcha"
    ],
    "suzuki alto": [
        "alto", "suzuki alto", "alto hatchbook", "alto 800", "alto k10",
        "alto std", "alto lx", "alto lxi", "alto vx", "alto vxi", "alto zx", "alto zxi",
        "alto works", "alto turbo", "alto ss40", "alto ca71v", "alto ha36s",
        "alto lapin", "alto hustle", "alto van", "alto 0.8", "alto 1.0",
        "celerio", "suzuki celerio", "a-star", "suzuki a-star", "pixis epoch",
        "daihatsu pixis epoch", "wagon r", "suzuki wagon r",
        "alt0", "suzuki alt0", "alto suzuki", "suzuky alto"
    ],
    "suzuki swift": [
        "swift", "suzuki swift", "swift hatchback", "swift 5dr", "swift 3dr",
        "swift gl", "swift gls", "swift glx", "swift ga", "swift rs", "swift sport",
        "swift gti", "swift dzire", "swift sedan", "swift 1.2", "swift 1.3", "swift 1.4",
        "swift manual", "swift automatico", "swift cvt", "swift turbo",
        "swift sf310", "swift sf413", "swift rs413", "swift rs415", "swift fz",
        "swift nz", "swift zc", "swift zd", "swift sport zc31s", "swift sport zc32s",
        "swft", "suzuki swft", "swift suzuki", "suzuky swift", "swyft"
    ],
    "hyundai accent": [
        "accent", "hyundai accent", "accent sedan", "accent hatchbook",
        "accent gl", "accent gls", "accent se", "accent limited", "accent rb", "accent verna",
        "accent blue", "accent era", "accent mc", "accent lc", "accent x3", "accent tagaz",
        "accent 1.4", "accent 1.6", "accent manual", "accent automatico",
        "verna", "hyundai verna", "brio", "hyundai brio", "pony", "hyundai pony",
        "excel", "hyundai excel", "solaris", "hyundai solaris", "rb15", "hyundai rb",
        "acent", "hyundai acent", "acsent", "hyundai acsent", "accent hyundai", "accen"
    ],
    "mitsubishi mirage": [
        "mirage", "mitsubishi mirage", "mirage hatchback", "mirage sedan",
        "mirage de", "mirage es", "mirage se", "mirage gt", "mirage ls", "mirage glx",
        "mirage gls", "mirage cyborg", "mirage asti", "mirage dingo", "mirage space star",
        "mirage 1.2", "mirage cvt", "mirage manual", "mirage automatico",
        "space star", "mitsubishi space star", "attrage", "mitsubishi attrage",
        "lancer mirage", "colt", "mitsubishi colt", "lancer cedia",
        "mirage mitsubishi", "mitsubishi mirage", "mirage 1.2", "miraje"
    ],
    "suzuki grand vitara": [
        "grand vitara", "suzuki grand vitara", "gran vitara", "suzuki gran vitara",
        "grand vitara suv", "grand vitara 4x4", "grandvitara",
        "grand vitara jlx", "grand vitara glx", "grand vitara sz", "grand vitara jx",
        "grand vitara xl-7", "grand vitara xl7", "grand vitara nomade", "grand vitara limited",
        "grand vitara se", "grand vitara premium", "grand vitara sport", "vitara 4x4",
        "grand vitara 2.0", "grand vitara 2.4", "grand vitara v6",
        "vitara", "suzuki vitara", "escudo", "suzuki escudo", "sidekick", "suzuki sidekick",
        "tracker", "geo tracker", "chevrolet tracker", "vitara brezza",
        "suzuki grandvitara", "grand bitara", "gran bitara", "vitara grand"
    ],
    "hyundai i10": [
        "i10", "hyundai i10", "i-10", "hyundai i-10", "i 10", "hyundai i 10",
        "i10 gl", "i10 gls", "i10 comfort", "i10 active", "i10 style", "i10 premium",
        "i10 classic", "i10 magna", "i10 sportz", "i10 asta", "i10 era", "i10 n line",
        "i10 1.0", "i10 1.1", "i10 1.2", "i10 manual", "i10 automatico",
        "atos", "hyundai atos", "atos prime", "hyundai atos prime", "santro",
        "hyundai santro", "santro xing", "grand i10", "hyundai grand i10",
        "hyundai i-10", "i10 hyundai", "hyundai 110", "hyundai l10"
    ],
    "kia rio": [
        "rio", "kia rio", "rio sedan", "rio hatchback", "rio 5", "rio5",
        "rio lx", "rio ex", "rio s", "rio sx", "rio x", "rio x-line", "rio xline",
        "rio hatch", "rio 1.4", "rio 1.6", "rio manual", "rio automatico", "rio cvt",
        "rio base", "rio sport", "rio premium", "rio comfort",
        "pride", "kia pride", "rio pride", "xceed", "kia xceed", "stonic", "kia stonic",
        "k2", "kia k2", "r7", "kia r7",
        "kia ryo", "rio kia", "kia rio5", "kia rio 5", "ryo", "kia rio x"
    ],
    "toyota": [
        "toyota", "toyoya", "toyota motor", "toyota motors", "toyota company",
        "toyota japan", "toyota auto", "toyota car", "toyota vehiculo",
        "toyoya", "toyotas", "toyata", "toyota"
    ],
    "honda": [
        "honda", "honda motor", "honda motors", "honda company", "honda japan",
        "honda auto", "honda car", "honda vehiculo", "honda motorcycle",
        "hondas", "honda motor company", "honda corp"
    ]
}

def create_model_year_pattern(sinonimos: Dict[str, List[str]]) -> re.Pattern:
    variantes = []
    for lista in sinonimos.values():
        variantes.extend(lista)
    modelos_escapados = [re.escape(v) for v in sorted(variantes, key=len, reverse=True)]
    modelos_union = '|'.join(modelos_escapados)
    pattern = rf"""
        \b(?P<y1>(?:19[8-9]\d|20[0-2]\d|\d{{2}}))\s+(?:{modelos_union})\b  |
        \b(?:{modelos_union})\s+(?P<y2>(?:19[8-9]\d|20[0-2]\d|\d{{2}}))\b
    """
    return re.compile(pattern, flags=re.IGNORECASE | re.VERBOSE)

_PATTERN_YEAR_AROUND_MODEL = create_model_year_pattern(sinonimos)

_PATTERN_YEAR_AROUND_KEYWORD = re.compile(
    r"(modelo|m/|versión|año|m\.|modelo:|año:|del|del:|md|md:)\s*[^\d]{0,5}([12]\d{3})", flags=re.IGNORECASE
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
        cur.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='anuncios'
        """)
        tabla_existe = cur.fetchone() is not None
        
        if not tabla_existe:
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
        
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}
        
        nuevas_columnas = {
            "relevante": "BOOLEAN DEFAULT 0",
            "confianza_precio": "TEXT DEFAULT 'baja'",
            "muestra_precio": "INTEGER DEFAULT 0",
            "año_asignado_inteligente": "BOOLEAN DEFAULT 0"  # NUEVA COLUMNA
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

def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{4,7}\b", s)
    candidatos = []
    for match in matches:
        num = int(match)
        if not (MIN_YEAR <= num <= MAX_YEAR):
            candidatos.append(num)
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
    
    for variante in variantes:
        pattern = rf"\b{re.escape(variante)}\b"
        if re.search(pattern, texto_limpio, re.IGNORECASE):
            return True
    return False

# NUEVA FUNCIÓN: Detectar modelo más frecuente
def detectar_modelo_mas_frecuente(texto: str, debug: bool = False) -> Optional[str]:
    """Detecta el modelo que más se repite en el texto"""
    contador_modelos = {}
    texto_lower = texto.lower()
    
    for modelo in MODELOS_INTERES:
        count = 0
        variantes = sinonimos.get(modelo, [modelo])
        
        for variante in variantes:
            pattern = rf'\b{re.escape(variante)}\b'
            matches = len(re.findall(pattern, texto_lower))
            count += matches
        
        if count > 0:
            contador_modelos[modelo] = count
    
    if debug and contador_modelos:
        print(f"🔍 Modelos detectados: {contador_modelos}")
    
    if contador_modelos:
        return max(contador_modelos.items(), key=lambda x: x[1])[0]
    return None

def es_candidato_año(raw: str) -> bool:
    orig = raw.strip()  
    if re.fullmatch(r"\d+\.\d+", orig):
        return False
    raw_limpio = orig.strip("'\"").replace(",", "").replace(".", "")
    if len(raw_limpio) > 4 or len(raw_limpio) < 2:
        return False
    if raw_limpio.startswith("00"):
        return False
    try:
        año = int(raw_limpio)
        if len(raw_limpio) == 2:
            año = 1900 + año if año > 50 else 2000 + año
        return MIN_YEAR <= año <= MAX_YEAR
    except ValueError:
        return False

# NUEVA FUNCIÓN: Validar que no sea precio duplicado
def validar_no_es_precio_duplicado(año_candidato: int, precio: int, texto: str, debug: bool = False) -> bool:
    """Valida que el año candidato no sea el precio duplicado"""
    if año_candidato == precio:
        if debug:
            print(f"❌ Año {año_candidato} descartado: coincide exactamente con precio")
        return False
    
    precio_str = str(precio)
    año_str = str(año_candidato)
    
    # Verificar si el año es parte del precio
    if precio_str.startswith(año_str) or precio_str.endswith(año_str):
        if debug:
            print(f"❌ Año {año_candidato} descartado: es parte del precio {precio}")
        return False
    
    # Buscar si el precio aparece múltiples veces
    apariciones_precio = len(re.findall(rf'\b{re.escape(precio_str)}\b', texto))
    if apariciones_precio > 1:
        pattern = rf'\b{año_candidato}\b'
        matches = list(re.finditer(pattern, texto))
        
        for match in matches:
            contexto = texto[max(0, match.start()-20):match.end()+20]
            if re.search(r'[Q$]', contexto):
                if debug:
                    print(f"❌ Año {año_candidato} descartado: aparece en contexto de precio")
                return False
    
    return True

# NUEVA FUNCIÓN: Obtener datos históricos del modelo
def obtener_datos_historicos_modelo(modelo: str, debug: bool = False) -> Dict[str, Any]:
    """Obtiene datos históricos del modelo para asignación inteligente de año"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT anio, precio, COUNT(*) as frecuencia
            FROM anuncios 
            WHERE modelo = ? AND anio IS NOT NULL AND precio > 0
            GROUP BY anio
            ORDER BY anio
        """, (modelo,))
        
        datos_raw = cur.fetchall()
        
        if not datos_raw:
            if debug:
                print(f"❌ Sin datos históricos para {modelo}")
            return {"suficientes_datos": False, "total_anuncios": 0}
        
        # Procesar datos
        años_con_datos = {}
        total_anuncios = 0
        
        for anio, precio_promedio, freq in datos_raw:
            # Obtener todos los precios del año específico
            cur.execute("""
                SELECT precio FROM anuncios 
                WHERE modelo = ? AND anio = ? AND precio > 0
            """, (modelo, anio))
            precios_año = [row[0] for row in cur.fetchall()]
            
            años_con_datos[anio] = precios_año
            total_anuncios += len(precios_año)
        
        # Calcular estadísticas por año
        estadisticas_por_año = {}
        for anio, precios in años_con_datos.items():
            if len(precios) >= 2:  # Mínimo 2 precios para estadísticas confiables
                precios_filtrados = filtrar_outliers(precios)
                estadisticas_por_año[anio] = {
                    "precio_min": min(precios_filtrados),
                    "precio_max": max(precios_filtrados),
                    "precio_promedio": statistics.mean(precios_filtrados),
                    "precio_mediana": statistics.median(precios_filtrados),
                    "cantidad_anuncios": len(precios_filtrados),
                    "precios": precios_filtrados
                }
        
        if debug:
            print(f"📊 {modelo}: {total_anuncios} anuncios, {len(años_con_datos)} años diferentes")
        
        suficientes_datos = total_anuncios >= MUESTRA_MINIMA_ASIGNACION_AÑO
        año_más_común = max(años_con_datos.items(), key=lambda x: len(x[1]))[0] if años_con_datos else None
        
        return {
            "suficientes_datos": suficientes_datos,
            "total_anuncios": total_anuncios,
            "años_únicos": len(años_con_datos),
            "estadisticas_por_año": estadisticas_por_año,
            "año_más_común": año_más_común
        }

# NUEVA FUNCIÓN: Calcular año probable por precio
def calcular_año_probable_por_precio(precio_objetivo: int, datos_historicos: Dict, debug: bool = False) -> Optional[int]:
    """Encuentra el año más probable basado en el precio del anuncio"""
    if not datos_historicos["suficientes_datos"]:
        return None
    
    candidatos_por_precio = []
    
    for anio, stats in datos_historicos["estadisticas_por_año"].items():
        precio_min = stats["precio_min"]
        precio_max = stats["precio_max"]
        precio_promedio = stats["precio_promedio"]
        cantidad = stats["cantidad_anuncios"]
        
        # Verificar si el precio objetivo cae en el rango de este año
        if precio_min <= precio_objetivo <= precio_max:
            distancia_promedio = abs(precio_objetivo - precio_promedio)
            confianza = cantidad
            score = confianza - (distancia_promedio / 1000)
            
            candidatos_por_precio.append({
                "anio": anio,
                "distancia": distancia_promedio,
                "confianza": confianza,
                "score": score
            })
            
            if debug:
                print(f"  📈 {anio}: rango Q{precio_min:,}-Q{precio_max:,}, distancia={distancia_promedio:,.0f}, score={score:.1f}")
    
    if not candidatos_por_precio:
        if debug:
            print("❌ Precio no coincide con ningún año histórico")
        return None
    
    mejor_candidato = max(candidatos_por_precio, key=lambda x: x["score"])
    if debug:
        print(f"🎯 Mejor candidato por precio: {mejor_candidato['anio']}")
    return mejor_candidato["anio"]

# NUEVA FUNCIÓN: Asignación inteligente de año
def asignar_año_inteligente(texto: str, modelo: str, precio: int, precio_oficial: Optional[int] = None, debug: bool = False) -> Optional[int]:
    """
    Sistema inteligente de asignación de año:
    1. Intenta extraer año del texto
    2. Si no encuentra año, usa datos históricos para asignar año probable
    """
    
    # PASO 1: Intentar extraer año del texto (caso normal)
    año_extraido = extraer_anio(texto, modelo=modelo, precio=precio, debug=debug)
    
    if año_extraido:
        # VALIDACIÓN ADICIONAL: Verificar que no sea precio duplicado
        if not validar_no_es_precio_duplicado(año_extraido, precio_oficial or precio, texto, debug):
            if debug:
                print("🔄 Año extraído descartado por validación de precio duplicado")
        else:
            if debug:
                print(f"✅ Año extraído del texto: {año_extraido}")
            return año_extraido
    
    if debug:
        print("🔍 No se encontró año confiable en texto. Intentando asignación inteligente...")
    
    # PASO 2: Obtener datos históricos del modelo
    datos_historicos = obtener_datos_historicos_modelo(modelo, debug)
    
    if not datos_historicos["suficientes_datos"]:
        if debug:
            print(f"❌ Datos insuficientes para {modelo} ({datos_historicos['total_anuncios']} anuncios < {MUESTRA_MINIMA_ASIGNACION_AÑO}). Descartando.")
        return None
    
    # PASO 3: MÉTODO COMBINADO - Año más común + Concordancia por precio
    año_más_común = datos_historicos["año_más_común"]
    año_por_precio = calcular_año_probable_por_precio(precio, datos_historicos, debug)
    
    if debug:
        print(f"📈 Año más común: {año_más_común}")
        print(f"💰 Año por precio: {año_por_precio}")
    
    # DECISIÓN INTELIGENTE
    if año_por_precio and año_más_común:
        if año_por_precio == año_más_común:
            if debug:
                print(f"🎯 ALTA CONFIANZA: Ambos métodos concuerdan en {año_más_común}")
            return año_más_común
        else:
            # Verificar compatibilidad del precio con el año más común
            stats_común = datos_historicos["estadisticas_por_año"].get(año_más_común, {})
            if stats_común and stats_común["precio_min"] <= precio <= stats_común["precio_max"]:
                if debug:
                    print(f"🎯 CONFIANZA MEDIA: Precio compatible con año más común {año_más_común}")
                return año_más_común
            else:
                if debug:
                    print(f"🎯 CONFIANZA MEDIA: Precio sugiere {año_por_precio}")
                return año_por_precio
    
    elif año_más_común:
        stats_común = datos_historicos["estadisticas_por_año"].get(año_más_común, {})
        if stats_común and stats_común["precio_min"] <= precio <= stats_común["precio_max"]:
            if debug:
                print(f"🎯 CONFIANZA BAJA: Solo año más común {año_más_común}")
            return año_más_común
    
    if debug:
        print("❌ No se pudo determinar año con suficiente confianza")
    return None

def extraer_anio(texto, modelo=None, precio=None, debug=False):
    if not texto or not isinstance(texto, str):
        if debug:
            print("❌ Texto inválido o vacío")
        return None
    
    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)  
    texto_original = texto
    texto = texto.lower()

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

    candidatos_prioritarios = []

    # 2) MÁXIMA PRIORIDAD: Patrones modelo-año específicos
    if modelo:
        modelo_variantes = sinonimos.get(modelo.lower(), [modelo.lower()])
        for variante in modelo_variantes:
            variante_escaped = re.escape(variante)
            
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
                    contexto = primera_linea[max(0, match.start()-20):match.end()+20]
                    if not re.search(rf'[q$]\s*{re.escape(raw)}', contexto, re.IGNORECASE):
                        candidatos_prioritarios.append((año, 800, "titulo"))
                        if debug:
                            print(f"📄 TITULO: {año}")
            except ValueError:
                continue

    # 5) BAJA PRIORIDAD: Búsqueda general
    if not any(prioridad >= 800 for _, prioridad, _ in candidatos_prioritarios):
        for match in re.finditer(r'\b(\d{2,4})\b', texto):
            raw = match.group(1)
            if es_candidato_año(raw):
                try:
                    año = int(raw)
                    año = normalizar_año_corto(año) if len(raw) == 2 else año
                    if MIN_YEAR <= año <= MAX_YEAR:
                        contexto = texto[max(0, match.start()-30):match.end()+30]
                        
                        if any(malo in contexto for malo in ['nacido', 'miembro desde', 'facebook', 'perfil']):
                            continue
                        
                        if re.search(rf'[q$]\s*{re.escape(raw)}', contexto, re.IGNORECASE):
                            continue
                            
                        candidatos_prioritarios.append((año, 100, "general"))
                        if debug:
                            print(f"🔍 GENERAL: {año}")
                except ValueError:
                    continue

    # SELECCIÓN FINAL
    if not candidatos_prioritarios:
        if debug:
            print("❌ No se encontraron candidatos")
        return None

    años_con_max_prioridad = {}
    for año, prioridad, fuente in candidatos_prioritarios:
        if año not in años_con_max_prioridad or prioridad > años_con_max_prioridad[año][0]:
            años_con_max_prioridad[año] = (prioridad, fuente)

    if debug:
        print("🎯 Candidatos finales:")
        for año, (prioridad, fuente) in sorted(años_con_max_prioridad.items(), key=lambda x: x[1][0], reverse=True):
            print(f"  - {año}: prioridad={prioridad}, fuente={fuente}")

    año_final = max(años_con_max_prioridad.items(), key=lambda x: x[1][0])[0]
    
    if debug:
        print(f"✅ Año seleccionado: {año_final}")
    
    return año_final

def validar_precio_coherente(precio: int, modelo: str, anio: int, texto: str = "") -> bool:
    if precio < 2000 or precio > 600000:
        return False

    ref_info = get_precio_referencia(modelo, anio)
    precio_ref = ref_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    muestra = ref_info.get("muestra", 0)

    if "reparar" in texto.lower() or "repuesto" in texto.lower():
        margen_bajo = 0.1 * precio_ref
    else:
        if muestra >= MUESTRA_MINIMA_CONFIABLE:
            margen_bajo = 0.25 * precio_ref
            margen_alto = 2.0 * precio_ref
        else:
            margen_bajo = 0.15 * precio_ref
            margen_alto = 2.5 * precio_ref
    
        return margen_bajo <= precio <= margen_alto

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

@timeit
def puntuar_anuncio(anuncio: Dict[str, Any]) -> int:
    score = 50

    texto = anuncio.get("texto", "")
    modelo = anuncio.get("modelo", "")
    anio = anuncio.get("anio", CURRENT_YEAR)
    precio = anuncio.get("precio", 0)

    if contiene_negativos(texto):
        score -= 40

    if es_extranjero(texto):
        score -= 30

    if not validar_precio_coherente(precio, modelo, anio, texto):
        score -= 50

    if anio > CURRENT_YEAR:
        score -= 60

    if _PATTERN_VEHICULAR_FUERTE.search(texto.lower()):
        score += 25

    palabras_buenas = ['vehículo', 'automático', 'standard', 'papeles al día', 
                       'excelente estado', 'poco kilometraje', 'original']
    for palabra in palabras_buenas:
        if palabra in texto.lower():
            score += 15
            break

    roi_info = get_precio_referencia(modelo, anio)
    precio_ref = roi_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    roi_valor = anuncio.get("roi", 0)
    confianza = roi_info.get("confianza", "baja")
    muestra = roi_info.get("muestra", 0)

    if roi_valor >= ROI_MINIMO * 2:
        score += 30
    elif roi_valor >= ROI_MINIMO:
        score += 20

    if precio < 0.7 * precio_ref:
        score += 25

    if confianza == "alta" and muestra >= MUESTRA_MINIMA_CONFIABLE:
        score += 15
    elif confianza == "media":
        score += 10

    if confianza == "baja" and roi_valor < 5:
        score -= 20

    if len(texto) > 300:
        score += 10
    elif len(texto) < 50:
        score -= 10

    return max(0, score)

@timeit
def insertar_anuncio_db(link, modelo, anio, precio, km, roi, score, relevante=False,
                        confianza_precio=None, muestra_precio=None, año_asignado_inteligente=False):
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("PRAGMA table_info(anuncios)")
    columnas_existentes = {row[1] for row in cur.fetchall()}
    
    if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio", "año_asignado_inteligente"]):
        cur.execute("""
        INSERT OR REPLACE INTO anuncios 
        (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, año_asignado_inteligente, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, año_asignado_inteligente))
    else:
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

def anuncio_diferente(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    campos_criticos = ["modelo", "anio", "precio"]
    campos_secundarios = ["km", "roi", "score"]
    
    for campo in campos_criticos:
        if str(a.get(campo, "")) != str(b.get(campo, "")):
            return True
    
    roi_a, roi_b = a.get("roi", 0), b.get("roi", 0)
    if abs(roi_a - roi_b) > 5:
        return True
        
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
        
        # NUEVA ESTADÍSTICA: Años asignados inteligentemente
        if "año_asignado_inteligente" in columnas_existentes:
            cur.execute("SELECT COUNT(*) FROM anuncios WHERE año_asignado_inteligente = 1")
            años_asignados = cur.fetchone()[0]
        else:
            años_asignados = 0
        
        cur.execute("""
            SELECT modelo, COUNT(*) FROM anuncios 
            GROUP BY modelo ORDER BY COUNT(*) DESC
        """)
        por_modelo = dict(cur.fetchall())
        
        return {
            "total_anuncios": total,
            "alta_confianza": alta_conf,
            "baja_confianza": baja_conf,
            "años_asignados_inteligente": años_asignados,
            "porcentaje_defaults": round((baja_conf / total) * 100, 1) if total else 0,
            "porcentaje_años_asignados": round((años_asignados / total) * 100, 1) if total else 0,
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

# NUEVA FUNCIÓN: Extraer datos de Facebook con validación de precio
def extraer_datos_facebook(post_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extrae datos específicos de Facebook donde precio viene en campo separado"""
    precio_campo = post_data.get('price', 0)
    titulo = post_data.get('title', '')
    descripcion = post_data.get('description', '')
    texto_completo = f"{titulo} {descripcion}"
    
    return {
        'precio_oficial': precio_campo,
        'texto': texto_completo,
        'titulo': titulo,
        'descripcion': descripcion
    }

# FUNCIÓN PRINCIPAL MEJORADA
def analizar_mensaje_con_asignacion_inteligente(texto: str, precio_oficial: Optional[int] = None, debug: bool = False) -> Optional[Dict[str, Any]]:
    """
    Versión mejorada de analizar_mensaje con asignación inteligente de año y detección de modelo más frecuente
    """
    if not texto or not isinstance(texto, str) or len(texto.strip()) < 10:
        if debug:
            print("❌ Texto inválido o demasiado corto")
        return None

    texto = limpiar_emojis_numericos(texto) 
    texto = normalizar_formatos_ano(texto)

    # PASO 1: Detectar modelo más frecuente (MEJORADO)
    modelo = detectar_modelo_mas_frecuente(texto, debug)
    if not modelo:
        if debug:
            print("❌ No se detectó ningún modelo válido")
        return None

    # PASO 2: Extraer precio
    precio = limpiar_precio(texto)
    if precio_oficial and precio_oficial > 0:
        precio = precio_oficial  # Usar precio oficial de Facebook si está disponible
    
    if not precio or precio < 2000:
        if debug:
            print(f"❌ Precio inválido: {precio}")
        return None

    # PASO 3: Asignación inteligente de año (NUEVA FUNCIONALIDAD)
    anio = asignar_año_inteligente(texto, modelo, precio, precio_oficial, debug)
    año_asignado_inteligente = False
    
    if not anio:
        if debug:
            print("❌ No se pudo asignar año confiable")
        return None
    
    # Verificar si el año fue asignado inteligentemente (no extraído del texto)
    año_extraido_original = extraer_anio(texto, modelo=modelo, precio=precio, debug=False)
    if not año_extraido_original:
        año_asignado_inteligente = True
        if debug:
            print(f"🤖 Año {anio} asignado inteligentemente (no estaba en el texto)")

    # PASO 4: Validar coherencia precio-modelo-año
    if not validar_precio_coherente(precio, modelo, anio):
        if debug:
            print(f"❌ Precio {precio} no coherente para {modelo} {anio}")
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
        "año_asignado_inteligente": año_asignado_inteligente,
        "roi_data": roi_data
    }

    if debug:
        print(f"✅ Análisis completado:")
        print(f"   Modelo: {modelo}")
        print(f"   Año: {anio} {'(asignado inteligentemente)' if año_asignado_inteligente else '(extraído del texto)'}")
        print(f"   Precio: Q{precio:,}")
        print(f"   ROI: {roi_data['roi']:.1f}%")
        print(f"   Score: {score}")
        print(f"   Relevante: {'Sí' if resultado['relevante'] else 'No'}")

    return resultado

# FUNCIÓN DE COMPATIBILIDAD: Mantener la función original para compatibilidad
def analizar_mensaje(texto: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    """
    Función original mantenida para compatibilidad.
    Redirige a la nueva función con asignación inteligente.
    """
    return analizar_mensaje_con_asignacion_inteligente(texto, debug=debug)

# FUNCIÓN ESPECÍFICA PARA FACEBOOK
def analizar_post_facebook(post_data: Dict[str, Any], debug: bool = False) -> Optional[Dict[str, Any]]:
    """
    Función específica para analizar posts de Facebook con precio en campo separado
    """
    datos_facebook = extraer_datos_facebook(post_data)
    
    resultado = analizar_mensaje_con_asignacion_inteligente(
        texto=datos_facebook['texto'],
        precio_oficial=datos_facebook['precio_oficial'],
        debug=debug
    )
    
    if resultado and debug:
        print(f"📘 Análisis Facebook - Precio oficial: Q{datos_facebook['precio_oficial']:,}")
    
    return resultado

# NUEVAS FUNCIONES DE UTILIDAD PARA ESTADÍSTICAS

def obtener_estadisticas_asignacion_inteligente(dias: int = 30) -> Dict[str, Any]:
    """Obtiene estadísticas sobre la asignación inteligente de años"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Verificar si existe la columna
        cur.execute("PRAGMA table_info(anuncios)")
        columnas = {row[1] for row in cur.fetchall()}
        
        if "año_asignado_inteligente" not in columnas:
            return {"error": "Columna año_asignado_inteligente no existe"}
        
        # Estadísticas generales
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN año_asignado_inteligente = 1 THEN 1 ELSE 0 END) as asignados_inteligente,
                AVG(CASE WHEN año_asignado_inteligente = 1 THEN score ELSE NULL END) as score_promedio_asignados,
                AVG(CASE WHEN año_asignado_inteligente = 0 THEN score ELSE NULL END) as score_promedio_extraidos
            FROM anuncios 
            WHERE fecha_scrape >= date('now', ?)
        """, (f"-{dias} days",))
        
        stats = cur.fetchone()
        
        # Por modelo
        cur.execute("""
            SELECT 
                modelo,
                COUNT(*) as total,
                SUM(CASE WHEN año_asignado_inteligente = 1 THEN 1 ELSE 0 END) as asignados,
                ROUND(AVG(CASE WHEN año_asignado_inteligente = 1 THEN roi ELSE NULL END), 1) as roi_promedio_asignados
            FROM anuncios 
            WHERE fecha_scrape >= date('now', ?)
            GROUP BY modelo
            HAVING COUNT(*) >= 5
            ORDER BY asignados DESC
        """, (f"-{dias} days",))
        
        por_modelo = cur.fetchall()
        
        return {
            "periodo_dias": dias,
            "total_anuncios": stats[0],
            "años_asignados_inteligente": stats[1],
            "porcentaje_asignados": round((stats[1] / stats[0]) * 100, 1) if stats[0] > 0 else 0,
            "score_promedio_asignados": round(stats[2] or 0, 1),
            "score_promedio_extraidos": round(stats[3] or 0, 1),
            "por_modelo": [
                {
                    "modelo": row[0],
                    "total": row[1], 
                    "asignados": row[2],
                    "porcentaje": round((row[2] / row[1]) * 100, 1),
                    "roi_promedio": row[3] or 0
                } 
                for row in por_modelo
            ]
        }

def obtener_modelos_con_datos_suficientes() -> List[Dict[str, Any]]:
    """Obtiene lista de modelos con suficientes datos para asignación inteligente"""
    modelos_info = []
    
    for modelo in MODELOS_INTERES:
        datos = obtener_datos_historicos_modelo(modelo)
        modelos_info.append({
            "modelo": modelo,
            "total_anuncios": datos["total_anuncios"],
            "años_únicos": datos.get("años_únicos", 0),
            "suficientes_datos": datos["suficientes_datos"],
            "año_más_común": datos.get("año_más_común")
        })
    
    return sorted(modelos_info, key=lambda x: x["total_anuncios"], reverse=True)

# FUNCIÓN DE PRUEBA Y VALIDACIÓN
def probar_asignacion_inteligente(textos_prueba: List[str], debug: bool = True) -> List[Dict[str, Any]]:
    """Función para probar la asignación inteligente con textos de ejemplo"""
    resultados = []
    
    print("🧪 Probando sistema de asignación inteligente de años...\n")
    
    for i, texto in enumerate(textos_prueba, 1):
        print(f"--- PRUEBA {i} ---")
        print(f"Texto: {texto[:100]}...")
        
        resultado = analizar_mensaje_con_asignacion_inteligente(texto, debug=debug)
        
        if resultado:
            resultados.append({
                "texto": texto,
                "modelo": resultado["modelo"],
                "año": resultado["año"],
                "precio": resultado["precio"],
                "roi": resultado["roi"],
                "score": resultado["score"],
                "año_asignado_inteligente": resultado["año_asignado_inteligente"],
                "relevante": resultado["relevante"]
            })
            print(f"✅ ÉXITO: {resultado['modelo']} {resultado['año']} - Q{resultado['precio']:,} (ROI: {resultado['roi']:.1f}%)")
            if resultado["año_asignado_inteligente"]:
                print("🤖 Año asignado inteligentemente")
        else:
            print("❌ No se pudo analizar")
            
        print()
    
    return resultados

# EJEMPLO DE USO CON TEXTOS DE PRUEBA
TEXTOS_PRUEBA_EJEMPLO = [
    "Honda Civic 2010 excelente estado Q45,000 negociable",  # Con año
    "Honda Civic excelente estado Q25,000 papeles al día",   # Sin año - debería asignar
    "Toyota Yaris Q30,000 automático full equipo",           # Sin año - debería asignar  
    "Chevrolet Spark Q15,000 poco kilometraje",              # Sin año - debería asignar
    "Ferrari F40 Q500,000 único dueño",                      # Modelo no reconocido
    "Honda Civic KIA SPORTAGE YARIS COROLLA descripción: Honda Civic 2008 Q28,000"  # Múltiples modelos
]

if __name__ == "__main__":
    # Inicializar base de datos
    inicializar_tabla_anuncios()
    
    # Mostrar estadísticas de modelos
    print("📊 Modelos con datos suficientes:")
    modelos = obtener_modelos_con_datos_suficientes()
    for modelo_info in modelos[:10]:  # Top 10
        status = "✅" if modelo_info["suficientes_datos"] else "❌"
        print(f"{status} {modelo_info['modelo']}: {modelo_info['total_anuncios']} anuncios, {modelo_info['años_únicos']} años")
    
    # Probar con textos de ejemplo
    # resultados = probar_asignacion_inteligente(TEXTOS_PRUEBA_EJEMPLO)
    
    print("\n🎉 Sistema de asignación inteligente listo para usar!")
    print(f"📋 Umbral mínimo para asignación: {MUESTRA_MINIMA_ASIGNACION_AÑO} anuncios")
    print("📘 Para Facebook, usar: analizar_post_facebook(post_data)")
    print("🔧 Para análisis general, usar: analizar_mensaje_con_asignacion_inteligente(texto)")
