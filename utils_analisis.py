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

# Configuración de pesos para scoring
WEIGHT_MODEL = 120      # Aumentado: año cerca del modelo es muy confiable
WEIGHT_TITLE = 110      # Mantenido: título es importante
WEIGHT_WINDOW = 100     # Aumentado: ventana del modelo es confiable
WEIGHT_GENERAL = 70     # Base para contexto general
WEIGHT_KEYWORD = 130    # Nuevo: año tras "modelo:", "año:", etc.

PENALTY_INVALID = -50   # Aumentado: contextos engañosos son muy malos
PENALTY_FUTURE = -40    # Nuevo: años futuros son problemáticos
PENALTY_NEGATIVE = -30  # Palabras negativas (repuestos, etc.)
PENALTY_FOREIGN = -20   # Lugares extranjeros

BONUS_VEHICULO = 20     # Aumentado: presencia de palabras vehículo
BONUS_PRECIO_COHERENTE = 15  # Nuevo: precio encaja con año/modelo
BONUS_CONTEXTO_FUERTE = 25   # Nuevo: contexto muy vehicular
BONUS_MARCA_MODELO = 10      # Nuevo: marca + modelo detectados

PRECIOS_POR_DEFECTO = {
    "yaris": 45000, "civic": 65000, "corolla": 50000, "sentra": 42000,
    "rav4": 130000, "cr-v": 95000, "tucson": 70000, "kia picanto": 35000,
    "chevrolet spark": 30000, "nissan march": 37000, "suzuki alto": 26000,
    "suzuki swift": 40000, "hyundai accent": 41000, "mitsubishi mirage": 33000,
    "suzuki grand vitara": 52000, "hyundai i10": 34000, "kia rio": 40000,
    "chevrolet tracker": 85000, "tracker": 85000,  # AGREGADO: Tracker específico
    "toyota": 48000, "honda": 50000, "chevrolet": 45000, "nissan": 42000
}

MODELOS_INTERES = list(PRECIOS_POR_DEFECTO.keys())

PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "motor fundido", "no arranca", "no enciende", "papeles atrasados",
    "sin motor", "para partes", "no funciona", "accidentado", "partes disponibles", 
    "partes", "solo piezas", "chatarra", "desguace"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador",
    "nicaragua", "venezuela", "brasil", "argentina"
]

# Patrones precompilados mejorados
_PATTERN_YEAR_FULL = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b")
_PATTERN_YEAR_SHORT = re.compile(r"['`´]?(\d{2})\b")
_PATTERN_YEAR_EMOJI = re.compile(r"([0-9️⃣]{4,8})")
_PATTERN_YEAR_SPECIAL = re.compile(r"\b(\d{1,2}[,.]\d{3})\b")

# MEJORA CRÍTICA: Sinónimos expandidos con mejor organización
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
        "yariz", "toyoya yaris", "toyota yariz", "yaris toyota"
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
        "civc", "civic honda", "honda civik", "civick"
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
        "pikanto", "kia pikanto", "picanto kia"
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
        "barina spark", "holden barina spark", "aveo hatchback", "chevrolet aveo hatchback",
        # Errores de escritura comunes
        "sp4rk", "chevrolet sp4rk", "spark chevrolet", "chevy sp4rk"
    ],
    
    "nissan march": [
        # Nombres oficiales
        "march", "nissan march", "march hatchback", "march 5dr",
        # Versiones específicas
        "march sense", "march advance", "march exclusive", "march sr", "march s",
        "march active", "march visia", "march acenta", "march tekna", "march nismo",
        "march 1.6", "march cvt", "march manual", "march automatico",
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
        "acent", "hyundai acent", "acsent", "hyundai acsent", "accent hyundai"
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
        "mirage mitsubishi", "miraje"
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
        # CORREGIDO: Tracker separado para evitar confusión
        # "tracker", "geo tracker", "chevrolet tracker", <- MOVIDO A SU PROPIA ENTRADA
        "vitara brezza",
        # Errores de escritura comunes
        "suzuki grandvitara", "grand bitara", "gran bitara", "vitara grand"
    ],
    
    # NUEVO: Chevrolet Tracker como entrada separada
    "chevrolet tracker": [
        "tracker", "chevrolet tracker", "chevy tracker", "geo tracker",
        "tracker suv", "tracker 4x4", "tracker awd", "tracker turbo",
        "tracker lt", "tracker ltz", "tracker ls", "tracker premier",
        "tracker crossover", "new tracker", "nuevo tracker",
        # Errores de escritura comunes
        "traker", "chevrolet traker", "chevy traker", "tracker chevrolet"
    ],
    
    # NUEVO: Tracker genérico que apunta a Chevrolet
    "tracker": [
        "tracker", "chevrolet tracker", "chevy tracker", "geo tracker",
        "tracker suv", "tracker 4x4", "new tracker"
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
        "kia ryo", "rio kia", "ryo", "kia rio x"
    ],
    
    "toyota": [
        # Nombres generales de marca
        "toyota", "toyota motor", "toyota motors", "toyota company",
        "toyota japan", "toyota auto", "toyota car", "toyota vehiculo",
        # Errores de escritura comunes
        "toyoya", "toyotas", "toyata"
    ],
    
    "honda": [
        # Nombres generales de marca
        "honda", "honda motor", "honda motors", "honda company", "honda japan",
        "honda auto", "honda car", "honda vehiculo",
        # Errores de escritura comunes
        "hondas", "honda motor company", "honda corp"
    ],
    
    "chevrolet": [
        # Nombres generales de marca
        "chevrolet", "chevy", "chevrolet motor", "chevrolet motors",
        "chevrolet company", "chevrolet auto", "chevrolet car",
        # Errores de escritura comunes
        "chevrolets", "chevrolet corp", "chebrolet"
    ],
    
    "nissan": [
        # Nombres generales de marca
        "nissan", "nissan motor", "nissan motors", "nissan company",
        "nissan japan", "nissan auto", "nissan car",
        # Errores de escritura comunes
        "nissans", "nissan corp", "nisan"
    ]
}

def create_model_year_pattern(sinonimos: Dict[str, List[str]]) -> re.Pattern:
    """Crea patrón mejorado para detectar año cerca del modelo"""
    variantes = []
    for lista in sinonimos.values():
        variantes.extend(lista)

    # Escapar y ordenar por longitud (más largos primero para mejor matching)
    modelos_escapados = sorted([re.escape(v) for v in variantes], key=len, reverse=True)
    modelos_union = '|'.join(modelos_escapados)

    # Patrón mejorado con más flexibilidad
    pattern = rf"""
        # Año de 2-4 dígitos antes del modelo (con separadores opcionales)
        \b(?P<y1>\d{{2,4}})[\s\-_]*(?:{modelos_union})\b  |
        # Modelo seguido de año (con separadores opcionales)  
        \b(?:{modelos_union})[\s\-_]*(?P<y2>\d{{2,4}})\b     |
        # Año entre paréntesis o corchetes después del modelo
        \b(?:{modelos_union})[\s]*[\(\[](?P<y3>\d{{2,4}})[\)\]]
    """

    return re.compile(pattern, flags=re.IGNORECASE | re.VERBOSE)

_PATTERN_YEAR_AROUND_MODEL = create_model_year_pattern(sinonimos)

# Patrón mejorado para keywords
_PATTERN_YEAR_AROUND_KEYWORD = re.compile(
    r"(modelo|m/|versión|año|m\.|modelo:|año:|del|del:|md|md:|version|version:)[^\d]{0,8}([12]\d{3})", 
    flags=re.IGNORECASE
)

_PATTERN_PRICE = re.compile(
    r"\b(?:q|\$)?\s*[\d.,]+(?:\s*quetzales?)?\b",
    flags=re.IGNORECASE
)

# Patrón mejorado para contextos inválidos
_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro desde|publicado en|nacido en|creado en|registro en|se unió a facebook en|perfil creado|calcomania|calcomania:|calcomania del|edad|años de edad|cumpleaños|nació|empleado desde)\b[^\d]{0,10}(19\d{2}|20\d{2})",
    flags=re.IGNORECASE
)

def timeit(func):
    """Decorador para medir tiempo de ejecución en modo DEBUG"""
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
    """Context manager mejorado para conexiones de base de datos"""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")  # Mejor concurrencia
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance rendimiento/seguridad
        yield conn
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

_conn: Optional[sqlite3.Connection] = None

def get_conn():
    """DEPRECATED: Usar get_db_connection() en su lugar"""
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return _conn

@timeit
def inicializar_tabla_anuncios():
    """Inicialización mejorada de la tabla con mejor manejo de errores"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Verificar si la tabla existe
            cur.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='anuncios'
            """)
            tabla_existe = cur.fetchone() is not None
            
            if not tabla_existe:
                # Crear tabla con estructura completa desde el inicio
                cur.execute("""
                    CREATE TABLE anuncios (
                        link TEXT PRIMARY KEY,
                        modelo TEXT NOT NULL,
                        anio INTEGER NOT NULL,
                        precio INTEGER NOT NULL,
                        km TEXT DEFAULT '',
                        fecha_scrape DATE DEFAULT (date('now')),
                        roi REAL DEFAULT 0.0,
                        score INTEGER DEFAULT 0,
                        relevante BOOLEAN DEFAULT 0,
                        confianza_precio TEXT DEFAULT 'baja',
                        muestra_precio INTEGER DEFAULT 0,
                        
                        -- Índices para mejor rendimiento
                        CHECK (anio >= 1980 AND anio <= 2030),
                        CHECK (precio > 0)
                    )
                """)
                
                # Crear índices para consultas frecuentes
                cur.execute("CREATE INDEX idx_modelo_anio ON anuncios(modelo, anio)")
                cur.execute("CREATE INDEX idx_score ON anuncios(score)")
                cur.execute("CREATE INDEX idx_fecha_scrape ON anuncios(fecha_scrape)")
                cur.execute("CREATE INDEX idx_relevante ON anuncios(relevante)")
                
                print("✅ Tabla anuncios creada con estructura completa e índices")
            else:
                # Verificar y agregar columnas faltantes
                cur.execute("PRAGMA table_info(anuncios)")
                columnas_existentes = {row[1] for row in cur.fetchall()}
                
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
            
    except sqlite3.Error as e:
        print(f"❌ Error al inicializar tabla: {e}")
        raise


def normalizar_formatos_ano(texto: str) -> str:
    """Convierte formatos como 2,009 o 2.009 → 2009"""
    return re.sub(r'\b(\d)[,\.](\d{3})\b', r'\1\2', texto)


def limpiar_emojis_numericos(texto: str) -> str:
    """Convierte emojis numéricos a dígitos normales"""
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
    """Limpia y valida enlaces"""
    if not link:
        return ""
    # Remover caracteres no ASCII y no imprimibles
    cleaned = ''.join(c for c in link.strip() if c.isascii() and c.isprintable())
    return cleaned


def contiene_negativos(texto: str) -> bool:
    """Verifica si el texto contiene palabras negativas (repuestos, partes, etc.)"""
    return any(palabra in texto.lower() for palabra in PALABRAS_NEGATIVAS)


def es_extranjero(texto: str) -> bool:
    """Verifica si el texto menciona lugares extranjeros"""
    return any(lugar in texto.lower() for lugar in LUGARES_EXTRANJEROS)


def validar_precio_coherente(precio: int, modelo: str, anio: int) -> bool:
    """
    Validación mejorada de precios con rangos más inteligentes
    """
    # Validación básica de rango
    if precio < 2000 or precio > 800000:
        return False

    try:
        ref_info = get_precio_referencia(modelo, anio)
        precio_ref = ref_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
        muestra = ref_info.get("muestra", 0)
        confianza = ref_info.get("confianza", "baja")

        # Ajustar tolerancia según la confianza de los datos
        if confianza == "alta" and muestra >= MUESTRA_MINIMA_CONFIABLE:
            # Rangos más estrictos con datos confiables
            margen_bajo = 0.25 * precio_ref
            margen_alto = 2.0 * precio_ref
        elif confianza == "media":
            # Rangos moderados
            margen_bajo = 0.20 * precio_ref
            margen_alto = 2.5 * precio_ref
        else:
            # Rangos muy permisivos para datos inciertos
            margen_bajo = 0.15 * precio_ref
            margen_alto = 3.0 * precio_ref

        # Ajustes por antigüedad del vehículo
        años_antiguedad = max(0, CURRENT_YEAR - anio)
        if años_antiguedad > 20:
            # Vehículos muy antiguos pueden ser muy baratos
            margen_bajo = min(margen_bajo, 3000)
        elif años_antiguedad < 3:
            # Vehículos nuevos no deberían ser demasiado baratos
            margen_bajo = max(margen_bajo, 0.4 * precio_ref)

        return margen_bajo <= precio <= margen_alto
        
    except Exception as e:
        if DEBUG:
            print(f"⚠️ Error en validar_precio_coherente: {e}")
        # En caso de error, usar validación básica
        precio_base = PRECIOS_POR_DEFECTO.get(modelo, 50000)
        return 0.1 * precio_base <= precio <= 4.0 * precio_base


def limpiar_precio(texto: str) -> int:
    """
    Extracción mejorada de precios con mejor filtrado de años
    """
    try:
        # Remover símbolos de moneda y limpiar
        texto_limpio = re.sub(r"[Qq\$\.,]", "", texto.lower())
        
        # Buscar números de 3-7 dígitos
        matches = re.findall(r"\b\d{3,7}\b", texto_limpio)
        
        if not matches:
            return 0
            
        # CORRECCIÓN CRÍTICA: Filtrar años correctamente
        candidatos_precio = []
        for match in matches:
            numero = int(match)
            # Excluir si es claramente un año
            if MIN_YEAR <= numero <= MAX_YEAR:
                continue
            # Excluir si es muy pequeño para ser precio
            if numero < 2000:
                continue
            # Excluir si es muy grande para ser precio realista
            if numero > 800000:
                continue
            candidatos_precio.append(numero)
        
        # Retornar el primer candidato válido
        return candidatos_precio[0] if candidatos_precio else 0
        
    except (ValueError, IndexError) as e:
        if DEBUG:
            print(f"⚠️ Error en limpiar_precio: {e}")
        return 0


def filtrar_outliers(precios: List[int]) -> List[int]:
    """Filtra outliers usando IQR mejorado"""
    if len(precios) < 4:
        return precios
        
    try:
        # Ordenar precios para cálculos
        precios_ordenados = sorted(precios)
        n = len(precios_ordenados)
        
        # Calcular cuartiles manualmente para mayor control
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = precios_ordenados[q1_idx]
        q3 = precios_ordenados[q3_idx]
        
        iqr = q3 - q1
        
        # Factor de outlier más conservador
        factor = 1.8  # Menos estricto que el 1.5 tradicional
        lim_inf = q1 - factor * iqr
        lim_sup = q3 + factor * iqr
        
        filtrados = [p for p in precios if lim_inf <= p <= lim_sup]
        
        # Retornar filtrados solo si quedan suficientes datos
        return filtrados if len(filtrados) >= max(2, len(precios) // 2) else precios
        
    except Exception as e:
        if DEBUG:
            print(f"⚠️ Error en filtrar_outliers: {e}")
        return precios


def coincide_modelo(texto: str, modelo: str) -> bool:
    """
    Detección mejorada de modelos con normalización avanzada
    """
    try:
        # Normalizar texto de entrada
        texto_norm = unicodedata.normalize("NFKD", texto.lower())
        texto_limpio = texto_norm.encode("ascii", "ignore").decode("ascii")
        
        # Obtener variantes del modelo
        modelo_lower = modelo.lower()
        variantes = sinonimos.get(modelo_lower, [modelo_lower])
        
        # MEJORA CRÍTICA: Búsqueda más inteligente
        for variante in variantes:
            variante_norm = unicodedata.normalize("NFKD", variante.lower())
            variante_limpia = variante_norm.encode("ascii", "ignore").decode("ascii")
            
            # Búsqueda exacta de palabra completa
            if re.search(rf'\b{re.escape(variante_limpia)}\b', texto_limpio):
                return True
                
            # Búsqueda flexible para modelos compuestos
            if len(variante_limpia.split()) > 1:
                # Para modelos como "chevrolet tracker", buscar ambas partes
                partes = variante_limpia.split()
                if all(parte in texto_limpio for parte in partes):
                    return True
        
        return False
        
    except Exception as e:
        if DEBUG:
            print(f"⚠️ Error en coincide_modelo: {e}")
        return False


def es_candidato_año(raw: str) -> bool:
    """Validación mejorada de candidatos a año"""
    if not raw or not raw.strip():
        return False
        
    orig = raw.strip()
    
    # Descartar decimales puros
    if re.fullmatch(r"\d+\.\d+", orig):
        return False
    
    # Limpiar separadores comunes
    limpio = orig.strip("'\"").replace(",", "").replace(".", "")
    
    # Validaciones de formato
    if len(limpio) > 4 or len(limpio) < 2:
        return False
        
    if limpio.startswith("00") or limpio.startswith("000"):
        return False
    
    # Convertir y validar rango
    try:
        año = int(limpio)
        
        # Para años de 2 dígitos, normalizar primero
        if len(limpio) == 2:
            año = 1900 + año if año > 50 else 2000 + año
            
        return MIN_YEAR <= año <= MAX_YEAR
        
    except ValueError:
        return False


def normalizar_año_corto(año: int) -> int:
    """Normaliza años de 2 dígitos a 4 dígitos"""
    if año >= 100:
        return año
    # Regla: >50 = 1900s, <=50 = 2000s
    return 1900 + año if año > 50 else 2000 + año


def calcular_score_año(año: int, contexto: str, fuente: str, modelo: str = None, precio: int = None) -> int:
    """
    Sistema de scoring mejorado para candidatos de año
    """
    # Score base según la fuente
    score_base = {
        'keyword': WEIGHT_KEYWORD,    # año: 2023, modelo: 2023
        'modelo': WEIGHT_MODEL,       # cerca del modelo detectado  
        'titulo': WEIGHT_TITLE,       # en el título del anuncio
        'ventana': WEIGHT_WINDOW,     # en ventana del modelo
        'texto': WEIGHT_GENERAL       # contexto general
    }
    
    score = score_base.get(fuente, WEIGHT_GENERAL)
    
    # PENALIZACIONES
    # Años futuros son muy sospechosos
    if año > CURRENT_YEAR:
        score += PENALTY_FUTURE
    
    # Contextos claramente no vehiculares
    contexto_lower = contexto.lower()
    if re.search(_PATTERN_INVALID_CTX, contexto):
        score += PENALTY_INVALID
    
    # Palabras que indican perfil/biografía
    bio_words = ['nacido', 'edad', 'años', 'miembro desde', 'se unió', 'perfil', 'usuario']
    if any(word in contexto_lower for word in bio_words):
        score += PENALTY_INVALID
    
    # BONIFICACIONES
    # Contexto fuertemente vehicular
    score_vehicular = calcular_score_contexto_vehicular(contexto, modelo)
    if score_vehicular > 15:
        score += BONUS_CONTEXTO_FUERTE
    elif score_vehicular > 8:
        score += BONUS_VEHICULO
    
    # Coherencia precio-año-modelo
    if precio and modelo:
        if validar_precio_coherente(precio, modelo, año):
            score += BONUS_PRECIO_COHERENTE
    
    # Marca + modelo detectados
    if modelo and contexto_lower:
        # Buscar marca en el contexto del año
        marcas = ['toyota', 'honda', 'nissan', 'chevrolet', 'hyundai', 'kia', 'suzuki', 'mitsubishi']
        if any(marca in contexto_lower for marca in marcas):
            score += BONUS_MARCA_MODELO
    
    return score


def calcular_score_contexto_vehicular(texto: str, modelo_detectado: str = None) -> int:
    """
    Calcula qué tan vehicular es el contexto
    """
    if not texto:
        return 0
        
    puntuacion = 0
    texto_lower = texto.lower()
    
    # BONUS MUY FUERTE: Modelo específico detectado
    if modelo_detectado and modelo_detectado in texto_lower:
        puntuacion += 15
    
    # Palabras vehiculares muy fuertes (+8 cada una)
    vehiculares_fuertes = [
        r'\b(modelo|año|del año|versión|m/)\b',
        r'\b(carro|auto|vehículo|camioneta|pickup|suv)\b',
        r'\b(motor|transmisión|mecánico|automático|standard)\b',
        r'\b(vendo|se vende|en venta)\b'
    ]
    
    # Palabras vehiculares moderadas (+4 cada una)
    vehiculares_moderadas = [
        r'\b(toyota|honda|nissan|ford|chevrolet|volkswagen|hyundai|kia|mazda|mitsubishi|suzuki)\b',
        r'\b(sedan|hatchback|coupe|wagon)\b',
        r'\b(kilometraje|km|millas|gasolina|diésel|combustible)\b',
        r'\b(papeles|documentos|traspaso|placas)\b'
    ]
    
    # Palabras vehiculares leves (+2 cada una)
    vehiculares_leves = [
        r'\b(usado|seminuevo|equipado|full|básico)\b',
        r'\b(llantas|frenos|batería|aceite|aire acondicionado)\b',
        r'\b(bien cuidado|excelente estado|poco uso|impecable)\b',
        r'\b(negociable|financiamiento|crédito)\b'
    ]
    
    # PENALIZACIONES (-8 cada una)
    penalizaciones_fuertes = [
        r'\b(casa|departamento|oficina|vivienda|terreno|local|apartamento)\b',
        r'\b(perfil|usuario|miembro|facebook|instagram|página|cuenta)\b',
        r'\b(teléfono|celular|contacto|whatsapp|email|correo)\b',
        r'\b(trabajo|empleo|empresa|oficina|estudios)\b'
    ]
    
    # Penalizaciones moderadas (-4 cada una)
    penalizaciones_moderadas = [
        r'\b(nacido|empleado|graduado|familia|matrimonio|pareja)\b',
        r'\b(publicado|creado|actualizado|visto|registrado)\b',
        r'\b(cumpleaños|aniversario|celebración|fiesta)\b'
    ]
    
    # Aplicar puntuaciones
    for patron in vehiculares_fuertes:
        puntuacion += 8 * len(re.findall(patron, texto_lower))
    
    for patron in vehiculares_moderadas:
        puntuacion += 4 * len(re.findall(patron, texto_lower))
        
    for patron in vehiculares_leves:
        puntuacion += 2 * len(re.findall(patron, texto_lower))
    
    for patron in penalizaciones_fuertes:
        puntuacion -= 8 * len(re.findall(patron, texto_lower))
        
    for patron in penalizaciones_moderadas:
        puntuacion -= 4 * len(re.findall(patron, texto_lower))
    
    return max(0, puntuacion)


@timeit
def extraer_anio(texto, modelo=None, precio=None, debug=False):
    """
    Extracción mejorada de año que recopila TODOS los candidatos antes de decidir
    """
    # Preparación del texto
    texto_procesado = limpiar_emojis_numericos(texto)
    texto_procesado = normalizar_formatos_ano(texto_procesado)
    texto_original = texto_procesado  # Guardar para correcciones manuales
    texto_lower = texto_procesado.lower()
    
    # Verificar correcciones manuales primero
    correccion_manual = obtener_correccion(texto_original)
    if correccion_manual:
        if debug:
            print(f"✅ Corrección manual aplicada: {texto_original[:50]}... → {correccion_manual}")
        return correccion_manual
    
    # Limpiar contextos claramente inválidos
    texto_limpio = _PATTERN_INVALID_CTX.sub("", texto_lower)
    
    candidatos = {}  # {año: score_máximo}
    
    def agregar_candidato(raw_año, contexto, fuente):
        """Agrega un candidato con su score"""
        if not es_candidato_año(raw_año):
            return
            
        try:
            año = int(raw_año.strip("'\""))
            año_normalizado = normalizar_año_corto(año) if año < 100 else año
            
            if MIN_YEAR <= año_normalizado <= MAX_YEAR:
                score = calcular_score_año(año_normalizado, contexto, fuente, modelo, precio)
                # Mantener el score más alto para cada año
                candidatos[año_normalizado] = max(candidatos.get(año_normalizado, 0), score)
                
                if debug:
                    print(f"  Candidato: {año_normalizado} (raw: {raw_año}) - Score: {score} - Fuente: {fuente}")
                    
        except (ValueError, TypeError) as e:
            if debug:
                print(f"  Error procesando candidato {raw_año}: {e}")
    
    # 1. BÚSQUEDA PRIORITARIA: Años con keywords específicos
    for match in _PATTERN_YEAR_AROUND_KEYWORD.finditer(texto_limpio):
        raw_año = match.group(2)
        contexto = texto_limpio[max(0, match.start()-20):match.end()+20]
        agregar_candidato(raw_año, contexto, 'keyword')
    
    # 2. BÚSQUEDA DE ALTA PRIORIDAD: Años cerca de modelos detectados
    if modelo:
        # Buscar modelo en el texto
        for variante in sinonimos.get(modelo.lower(), [modelo.lower()]):
            pattern = re.compile(rf'\b{re.escape(variante)}\b', re.IGNORECASE)
            for match in pattern.finditer(texto_limpio):
                # Extraer ventana alrededor del modelo
                inicio = max(0, match.start() - 40)
                fin = min(len(texto_limpio), match.end() + 40)
                ventana = texto_limpio[inicio:fin]
                
                # Buscar años en la ventana
                for año_match in re.finditer(r"(?:'|')?(\d{2,4})", ventana):
                    raw_año = año_match.group(1)
                    contexto_ventana = ventana[max(0, año_match.start()-15):año_match.end()+15]
                    agregar_candidato(raw_año, contexto_ventana, 'modelo')
    
    # 3. BÚSQUEDA GENERAL: Años en todo el texto
    # Primero el título (primera línea)
    lineas = texto_limpio.split('\n')
    if lineas:
        titulo = lineas[0]
        for match in re.finditer(r"(?:'|')?(\d{2,4})", titulo):
            raw_año = match.group(1)
            contexto = titulo[max(0, match.start()-20):match.end()+20]
            agregar_candidato(raw_año, contexto, 'titulo')
    
    # Luego el resto del texto
    for match in re.finditer(r"(?:'|')?(\d{2,4})", texto_limpio):
        raw_año = match.group(1)
        contexto = texto_limpio[max(0, match.start()-25):match.end()+25]
        agregar_candidato(raw_año, contexto, 'texto')
    
    # 4. SELECCIÓN DEL MEJOR CANDIDATO
    if not candidatos:
        if debug:
            print("❌ No se encontraron candidatos válidos")
        return None
    
    # Ordenar candidatos por score
    candidatos_ordenados = sorted(candidatos.items(), key=lambda x: x[1], reverse=True)
    
    if debug:
        print(f"🎯 Candidatos finales encontrados ({len(candidatos)}):")
        for año, score in candidatos_ordenados[:5]:  # Mostrar top 5
            print(f"  - {año}: score {score}")
    
    # Seleccionar el mejor candidato
    mejor_año, mejor_score = candidatos_ordenados[0]
    
    # Umbral mínimo de confianza
    umbral_minimo = 60
    if mejor_score < umbral_minimo:
        if debug:
            print(f"❌ Mejor candidato {mejor_año} tiene score {mejor_score} < {umbral_minimo}")
        return None
    
    if debug:
        print(f"✅ Año seleccionado: {mejor_año} (score: {mejor_score})")
    
    return mejor_año


@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """Obtiene precio de referencia con estadísticas mejoradas"""
    if tolerancia is None:
        tolerancia = TOLERANCIA_PRECIO_REF
        
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT precio FROM anuncios 
                WHERE modelo = ? AND ABS(anio - ?) <= ? AND precio > 0
                ORDER BY precio
            """, (modelo, anio, tolerancia))
            
            precios = [row[0] for row in cur.fetchall()]
            
        if not precios:
            return {
                "precio": PRECIOS_POR_DEFECTO.get(modelo, 50000),
                "confianza": "baja",
                "muestra": 0,
                "rango": "default"
            }
        
        # Filtrar outliers para mejor calidad
        if len(precios) >= 4:
            precios_filtrados = filtrar_outliers(precios)
        else:
            precios_filtrados = precios
        
        mediana = statistics.median(precios_filtrados)
        
        # Determinar nivel de confianza
        if len(precios_filtrados) >= MUESTRA_MINIMA_CONFIABLE:
            confianza = "alta"
        elif len(precios_filtrados) >= MUESTRA_MINIMA_MEDIA:
            confianza = "media"
        else:
            confianza = "baja"
            
        return {
            "precio": int(mediana),
            "confianza": confianza,
            "muestra": len(precios_filtrados),
            "rango": f"{min(precios_filtrados)}-{max(precios_filtrados)}"
        }
        
    except Exception as e:
        if DEBUG:
            print(f"⚠️ Error en get_precio_referencia: {e}")
        return {
            "precio": PRECIOS_POR_DEFECTO.get(modelo, 50000),
            "confianza": "baja",
            "muestra": 0,
            "rango": "error"
        }


@timeit  
def calcular_roi_real(modelo: str, precio_compra: int, anio: int, costo_extra: int = 2000) -> Dict[str, Any]:
    """Cálculo mejorado de ROI con depreciación más realista"""
    try:
        ref = get_precio_referencia(modelo, anio)
        años_antiguedad = max(0, CURRENT_YEAR - anio)
        
        # Curva de depreciación más realista
        # Depreciación más fuerte en los primeros años, luego se estabiliza
        if años_antiguedad <= 3:
            factor_depreciacion = (1 - 0.15) ** años_antiguedad  # 15% anual primeros 3 años
        elif años_antiguedad <= 10:
            factor_depreciacion = (1 - 0.15) ** 3 * (1 - 0.08) ** (años_antiguedad - 3)  # 8% anual años 4-10
        else:
            factor_depreciacion = (1 - 0.15) ** 3 * (1 - 0.08) ** 7 * (1 - 0.04) ** (años_antiguedad - 10)  # 4% anual después de 10 años
        
        precio_depreciado = ref["precio"] * factor_depreciacion
        inversion_total = precio_compra + costo_extra
        
        roi = ((precio_depreciado - inversion_total) / inversion_total) * 100 if inversion_total > 0 else 0.0
        
        return {
            "roi": round(roi, 1),
            "precio_referencia": ref["precio"],
            "precio_depreciado": int(precio_depreciado),
            "confianza": ref["confianza"],
            "muestra": ref["muestra"], 
            "inversion_total": inversion_total,
            "años_antiguedad": años_antiguedad,
            "factor_depreciacion": round(factor_depreciacion, 3)
        }
        
    except Exception as e:
        if DEBUG:
            print(f"⚠️ Error en calcular_roi_real: {e}")
        return {
            "roi": 0.0,
            "precio_referencia": PRECIOS_POR_DEFECTO.get(modelo, 50000),
            "precio_depreciado": 0,
            "confianza": "error",
            "muestra": 0,
            "inversion_total": precio_compra + costo_extra,
            "años_antiguedad": max(0, CURRENT_YEAR - anio)
        }


@timeit
def puntuar_anuncio(anuncio: Dict[str, Any]) -> int:
    """
    Sistema de puntuación mejorado y más coherente
    """
    score = 0
    
    texto = anuncio.get("texto", "")
    modelo = anuncio.get("modelo", "")
    anio = anuncio.get("anio", CURRENT_YEAR)
    precio = anuncio.get("precio", 0)
    roi = anuncio.get("roi", 0)
    
    # 1. PENALIZACIONES FUERTES
    if contiene_negativos(texto):
        score += PENALTY_NEGATIVE  # -30
        
    if es_extranjero(texto):
        score += PENALTY_FOREIGN  # -20
        
    if not validar_precio_coherente(precio, modelo, anio):
        score += PENALTY_INVALID  # -50
        
    if anio > CURRENT_YEAR:
        score += PENALTY_FUTURE  # -40
    
    # 2. BONIFICACIONES POR CONTEXTO VEHICULAR
    score_vehicular = calcular_score_contexto_vehicular(texto, modelo)
    if score_vehicular > 20:
        score += BONUS_CONTEXTO_FUERTE  # +25
    elif score_vehicular > 10:
        score += BONUS_VEHICULO  # +20
    elif score_vehicular > 5:
        score += 10  # Bonus menor
    
    # 3. BONIFICACIONES POR ROI Y DATOS
    try:
        roi_info = get_precio_referencia(modelo, anio)
        confianza = roi_info.get("confianza", "baja")
        muestra = roi_info.get("muestra", 0)
        precio_ref = roi_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
        
        # ROI excelente
        if roi >= ROI_MINIMO * 2:  # ROI >= 20%
            score += 30
        elif roi >= ROI_MINIMO:  # ROI >= 10%
            score += 20
        elif roi >= 5:  # ROI >= 5%
            score += 10
        
        # Ganga detectada (precio muy por debajo del mercado)
        if precio < 0.7 * precio_ref:
            score += 25  # Bonus por ganga
        elif precio < 0.85 * precio_ref:
            score += 15  # Bonus por buen precio
            
        # Confianza en los datos
        if confianza == "alta" and muestra >= MUESTRA_MINIMA_CONFIABLE:
            score += 15
        elif confianza == "media":
            score += 10
        else:
            score -= 5  # Penalización por baja confianza
            
    except Exception as e:
        if DEBUG:
            print(f"⚠️ Error en cálculos ROI para puntuación: {e}")
        score -= 10  # Penalización por error en datos
    
    # 4. BONIFICACIONES POR CALIDAD DEL ANUNCIO
    if len(texto) > 500:
        score += 10  # Anuncio detallado
    elif len(texto) > 200:
        score += 5   # Anuncio moderadamente detallado
        
    # URLs presentes (indica anuncio completo)
    if re.search(r'https?://', texto):
        score += 5
    
    # 5. AJUSTES FINALES
    # Vehículos muy antiguos tienen score reducido
    if anio < CURRENT_YEAR - 25:
        score -= 10
        
    # Vehículos muy nuevos sin ROI alto son sospechosos
    if anio >= CURRENT_YEAR - 2 and roi < 5:
        score -= 15
    
    return score


@timeit
def insertar_anuncio_db(link, modelo, anio, precio, km, roi, score, relevante=False,
                        confianza_precio=None, muestra_precio=None):
    """Inserción mejorada con mejor manejo de errores"""
    try:
        link_limpio = limpiar_link(link)
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Verificar columnas existentes
            cur.execute("PRAGMA table_info(anuncios)")
            columnas_existentes = {row[1] for row in cur.fetchall()}
            
            if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]):
                # Insertar con todas las columnas
                cur.execute("""
                INSERT OR REPLACE INTO anuncios 
                (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, 
                 muestra_precio, fecha_scrape)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
                """, (link_limpio, modelo, anio, precio, km, roi, score, relevante, 
                      confianza_precio, muestra_precio))
            else:
                # Insertar solo columnas básicas
                cur.execute("""
                INSERT OR REPLACE INTO anuncios 
                (link, modelo, anio, precio, km, roi, score, fecha_scrape)
                VALUES (?, ?, ?, ?, ?, ?, ?, DATE('now'))
                """, (link_limpio, modelo, anio, precio, km, roi, score))
            
            conn.commit()
            
    except sqlite3.Error as e:
        print(f"❌ Error al insertar anuncio: {e}")
        raise


def existe_en_db(link: str) -> bool:
    """Verifica si un anuncio ya existe en la base de datos"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
            return cur.fetchone() is not None
    except sqlite3.Error as e:
        if DEBUG:
            print(f"⚠️ Error verificando existencia en DB: {e}")
        return False


@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    """Calcula el rendimiento de un modelo en los últimos días"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) as rendimiento
                FROM anuncios 
                WHERE modelo = ? AND fecha_scrape >= date('now', ?)
            """, (SCORE_MIN_DB, modelo, f"-{dias} days"))
            
            result = cur.fetchone()
            return round(result[0] if result and result[0] else 0.0, 3)
            
    except sqlite3.Error as e:
        if DEBUG:
            print(f"⚠️ Error calculando rendimiento: {e}")
        return 0.0


@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    """Identifica modelos con bajo rendimiento"""
    return [modelo for modelo in MODELOS_INTERES 
            if get_rendimiento_modelo(modelo, dias) < threshold]


def get_estadisticas_db() -> Dict[str, Any]:
    """Obtiene estadísticas completas de la base de datos"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Total de anuncios
            cur.execute("SELECT COUNT(*) FROM anuncios")
            total = cur.fetchone()[0]
            
            if total == 0:
                return {
                    "total_anuncios": 0,
                    "alta_confianza": 0,
                    "baja_confianza": 0,
                    "porcentaje_defaults": 0,
                    "por_modelo": {}
                }
            
            # Verificar columnas existentes
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
            
            # Estadísticas por modelo
            cur.execute("""
                SELECT modelo, COUNT(*) as cantidad
                FROM anuncios 
                GROUP BY modelo 
                ORDER BY cantidad DESC
            """)
            por_modelo = dict(cur.fetchall())
            
            # Estadísticas adicionales
            cur.execute("SELECT AVG(score) FROM anuncios WHERE score > 0")
            score_promedio = cur.fetchone()[0] or 0
            
            cur.execute("SELECT COUNT(*) FROM anuncios WHERE score >= ?", (SCORE_MIN_TELEGRAM,))
            relevantes = cur.fetchone()[0]
            
            return {
                "total_anuncios": total,
                "alta_confianza": alta_conf,
                "baja_confianza": baja_conf,
                "porcentaje_defaults": round((baja_conf / total) * 100, 1),
                "por_modelo": por_modelo,
                "score_promedio": round(score_promedio, 1),
                "anuncios_relevantes": relevantes,
                "porcentaje_relevantes": round((relevantes / total) * 100, 1)
            }
            
    except sqlite3.Error as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return {
            "total_anuncios": 0,
            "error": str(e)
        }


def obtener_anuncio_db(link: str) -> Optional[Dict[str, Any]]:
    """Obtiene un anuncio específico de la base de datos"""
    try:
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
            
    except sqlite3.Error as e:
        if DEBUG:
            print(f"⚠️ Error obteniendo anuncio: {e}")
        return None


def anuncio_diferente(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Compara si dos anuncios son diferentes en campos clave"""
    if not a or not b:
        return True
        
    campos_clave = ["modelo", "anio", "precio", "km", "roi", "score"]
    return any(str(a.get(campo, "")) != str(b.get(campo, "")) for campo in campos_clave)


def detectar_modelo_mejorado(texto: str) -> Optional[str]:
    """
    Detección mejorada de modelos con priorización inteligente
    """
    if not texto:
        return None
        
    texto_norm = unicodedata.normalize("NFKD", texto.lower())
    texto_limpio = texto_norm.encode("ascii", "ignore").decode("ascii")
    
    candidatos = []
    
    # Buscar todos los modelos posibles
    for modelo_clave, variantes in sinonimos.items():
        for variante in variantes:
            variante_norm = unicodedata.normalize("NFKD", variante.lower())
            variante_limpia = variante_norm.encode("ascii", "ignore").decode("ascii")
            
            # Buscar coincidencias exactas de palabra completa
            if re.search(rf'\b{re.escape(variante_limpia)}\b', texto_limpio):
                # Calcular score de la coincidencia
                score = len(variante_limpia)  # Más específico = mejor score
                
                # Bonus si es el modelo exacto (no una variante)
                if variante_limpia == modelo_clave:
                    score += 10
                    
                # Bonus si incluye marca
                if len(variante_limpia.split()) > 1:
                    score += 5
                    
                candidatos.append((modelo_clave, score, variante))
    
    if not candidatos:
        return None
    
    # MEJORA CRÍTICA: Resolver conflictos de modelos
    # Ordenar por score y resolver ambigüedades
    candidatos.sort(key=lambda x: x[1], reverse=True)
    
    # Si el mejor candidato es significativamente mejor, usarlo
    if len(candidatos) == 1 or candidatos[0][1] > candidatos[1][1] + 5:
        return candidatos[0][0]
    
    # Si hay empate, usar lógica especial
    mejores = [c for c in candidatos if c[1] == candidatos[0][1]]
    
    # Priorizar modelos específicos sobre genéricos
    especificos = [c for c in mejores if c[0] not in ['toyota', 'honda', 'chevrolet', 'nissan']]
    if especificos:
        return especificos[0][0]
    
    # Si todos son genéricos, tomar el primero
    return mejores[0][0]


def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    """
    Función principal mejorada para analizar mensajes de anuncios
    """
    if not texto or len(texto.strip()) < 20:
        return None
    
    try:
        # Preprocesamiento del texto
        texto_procesado = limpiar_emojis_numericos(texto)
        texto_procesado = normalizar_formatos_ano(texto_procesado)
        
        # Extracción de datos básicos
        precio = limpiar_precio(texto_procesado)
        if precio == 0:
            return None
            
        # Detección mejorada de modelo
        modelo = detectar_modelo_mejorado(texto_procesado)
        if not modelo:
            return None
            
        # Extracción mejorada de año con debug condicional
        anio = extraer_anio(texto_procesado, modelo=modelo, precio=precio, debug=DEBUG)
        if not anio:
            return None
            
        # Validaciones
        if not validar_precio_coherente(precio, modelo, anio):
            if DEBUG:
                print(f"❌ Precio {precio} no coherente para {modelo} {anio}")
            return None
        
        # Verificar que no contenga demasiadas palabras negativas
        if contiene_negativos(texto_procesado):
            score_vehicular = calcular_score_contexto_vehicular(texto_procesado, modelo)
            if score_vehicular < 10:  # Si el contexto no es suficientemente vehicular
                if DEBUG:
                    print(f"❌ Texto contiene palabras negativas y poco contexto vehicular")
                return None
        
        # Cálculos avanzados
        roi_data = calcular_roi_real(modelo, precio, anio)
        
        # Crear objeto de anuncio para scoring
        anuncio_obj = {
            "texto": texto_procesado,
            "modelo": modelo,
            "anio": anio,
            "precio": precio,
            "roi": roi_data.get("roi", 0)
        }
        
        score = puntuar_anuncio(anuncio_obj)
        
        # Extraer URL si existe
        url_match = re.search(r'https?://[^\s]+', texto)
        url = url_match.group(0) if url_match else ""
        
        # Determinar relevancia
        relevante = (score >= SCORE_MIN_TELEGRAM and 
                    roi_data["roi"] >= ROI_MINIMO and
                    roi_data["confianza"] != "error")
        
        resultado = {
            "url": limpiar_link(url),
            "modelo": modelo,
            "año": anio,
            "precio": precio,
            "roi": roi_data["roi"],
            "score": score,
            "relevante": relevante,
            "km": "",  # Se puede extraer en futuras mejoras
            "confianza_precio": roi_data["confianza"],
            "muestra_precio": roi_data["muestra"],
            "roi_data": roi_data
        }
        
        if DEBUG:
            print(f"✅ Anuncio analizado: {modelo} {anio} - Q{precio:,} - ROI: {roi_data['roi']}% - Score: {score}")
        
        return resultado
        
    except Exception as e:
        if DEBUG:
            print(f"❌ Error analizando mensaje: {e}")
            print(f"Texto problemático: {texto[:100]}...")
        return None


# Funciones auxiliares para retrocompatibilidad y mantenimiento

def limpiar_base_datos(dias_antiguos: int = 30):
    """Limpia registros antiguos de la base de datos"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM anuncios 
                WHERE fecha_scrape < date('now', ?)
            """, (f"-{dias_antiguos} days",))
            
            eliminados = cur.rowcount
            conn.commit()
            
            print(f"✅ Eliminados {eliminados} registros antiguos")
            return eliminados
            
    except sqlite3.Error as e:
        print(f"❌ Error limpiando base de datos: {e}")
        return 0


def exportar_estadisticas_detalladas() -> Dict[str, Any]:
    """Exporta estadísticas detalladas para análisis"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Estadísticas por modelo y año
            cur.execute("""
                SELECT modelo, anio, COUNT(*) as cantidad, 
                       AVG(precio) as precio_promedio,
                       AVG(roi) as roi_promedio,
                       AVG(score) as score_promedio
                FROM anuncios 
                GROUP BY modelo, anio
                HAVING cantidad > 1
                ORDER BY modelo, anio
            """)
            
            stats_detalladas = []
            for row in cur.fetchall():
                stats_detalladas.append({
                    "modelo": row[0],
                    "anio": row[1],
                    "cantidad": row[2],
                    "precio_promedio": round(row[3], 0),
                    "roi_promedio": round(row[4], 1),
                    "score_promedio": round(row[5], 1)
                })
            
            # Mejores oportunidades (alto ROI, buen score)
            cur.execute("""
                SELECT modelo, anio, precio, roi, score, link
                FROM anuncios 
                WHERE roi >= ? AND score >= ?
                ORDER BY roi DESC, score DESC
                LIMIT 20
            """, (ROI_MINIMO, SCORE_MIN_TELEGRAM))
            
            mejores_oportunidades = []
            for row in cur.fetchall():
                mejores_oportunidades.append({
                    "modelo": row[0],
                    "anio": row[1],
                    "precio": row[2],
                    "roi": row[3],
                    "score": row[4],
                    "link": row[5]
                })
            
            return {
                "estadisticas_detalladas": stats_detalladas,
                "mejores_oportunidades": mejores_oportunidades,
                "timestamp": datetime.now().isoformat()
            }
            
    except sqlite3.Error as e:
        print(f"❌ Error exportando estadísticas: {e}")
        return {"error": str(e)}


# Validaciones y tests unitarios básicos
def test_extraccion_basica():
    """Test básico de las funciones principales"""
    tests = [
        {
            "texto": "Toyota Yaris 2018 Q16,000 excelente estado",
            "esperado": {"modelo": "yaris", "año": 2018, "precio": 16000}
        },
        {
            "texto": "Chevrolet Tracker 2020 Q85,000 seminuevo",
            "esperado": {"modelo": "chevrolet tracker", "año": 2020, "precio": 85000}
        },
        {
            "texto": "Honda Civic 2015 $18,500 negociable",
            "esperado": {"modelo": "civic", "año": 2015, "precio": 18500}
        }
    ]
    
    print("🧪 Ejecutando tests básicos...")
    
    for i, test in enumerate(tests, 1):
        resultado = analizar_mensaje(test["texto"])
        
        if resultado:
            exito = (resultado["modelo"] == test["esperado"]["modelo"] and
                    resultado["año"] == test["esperado"]["año"] and
                    resultado["precio"] == test["esperado"]["precio"])
            
            print(f"Test {i}: {'✅ PASS' if exito else '❌ FAIL'}")
            if not exito:
                print(f"  Esperado: {test['esperado']}")
                print(f"  Obtenido: {{'modelo': '{resultado['modelo']}', 'año': {resultado['año']}, 'precio': {resultado['precio']}}}")
        else:
            print(f"Test {i}: ❌ FAIL - No se pudo analizar")
    
    print("🧪 Tests completados")


if __name__ == "__main__":
    # Ejecutar tests si se ejecuta directamente
    inicializar_tabla_anuncios()
    test_extraccion_basica()
    print(f"📊 Estadísticas actuales: {get_estadisticas_db()}")
