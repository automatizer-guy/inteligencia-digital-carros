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

PENALTY_INVALID   = -50    # contextos engañosos: nacido, edad, etc.
BONUS_VEHICULO    =  10    # presencia de palabras vehículo
BONUS_PRECIO_HIGH =   5    # bonus si precio encaja con año
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

_PATTERN_YEAR_KEYWORD_STRONG = re.compile(
    r"(modelo|m/|versión|año|m.|modelo:|año:|del|del:)[^\d]{0,5}([12]\d{3})", flags=re.IGNORECASE
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
    precio_ref = PRECIOS_POR_DEFECTO.get(modelo, 50000)
    return 0.2 * precio_ref <= precio <= 2.5 * precio_ref

def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{3,7}\b", s)
    año_actual = datetime.now().year
    candidatos = [int(x) for x in matches if int(x) < 1990 or int(x) > año_actual + 1]
    return candidatos[0] if candidatos else 0

def filtrar_outliers(precios: List[int]) -> List[int]:
    if len(precios) < 4:
        return precios
    try:
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
        filtrados = [p for p in precios if lim_inf <= p <= lim_sup]
        return filtrados if len(filtrados) >= 2 else precios
    except:
        return precios

def coincide_modelo(texto: str, modelo: str) -> bool:
    texto_l = unicodedata.normalize("NFKD", texto.lower())
    modelo_l = modelo.lower()
    sinonimos = {
        "accent": ["acent", "acsent", "accent rb", "hyundai rb", "rb15", "hyundai acent", "accen"],
        "civic": ["civc", "civic lx", "civic ex", "civic sport", "cvic", "civic 1.8", "honda civic"],
        "sentra": ["sentran", "sentra b13", "nissan b13", "nissan sentra", "sentr4", "sentra clásico", "Nissan Sentra GXE"],
        "rio": ["rio5", "kia rio", "rio lx", "rio x", "rio x-line", "kia hatchback", "kia ryo"],
        "swift": ["swift sport", "swift gl", "suzuki swift", "Suzuki swift gti", "swift dzire", "swft", "swift 1.2"],
        "march": ["nissan march", "march active", "march sense", "m4rch"],
        "yaris": ["toyota yaris", "yaris hb", "yariz", "yaris core", "yaris s"],
        "cr-v": ["crv", "cr-v lx", "honda cr-v", "cr b", "crv ex", "crv turbo"],
        "tucson": ["hyundai tucson", "tucsón", "tuczon", "tucson gls", "tucson ix"],
        "spark": ["chevrolet spark", "spark gt", "sp4rk", "spark life"],
        "picanto": ["kia picanto", "picanto xline", "pikanto", "picanto 1.2"],
        "alto": ["suzuki alto", "alto 800", "alt0", "alto std"],
        "grand vitara": ["suzuki grand vitara", "gran vitara", "vitara 4x4", "grandvitara"]
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
            raw = m.group("y")
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
            
    if not candidatos or max(candidatos.values()) < 60:
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
def puntuar_anuncio(texto: str, roi_info: Optional[Dict] = None) -> int:
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    if not (modelo and anio and precio):
        return 0
    if not validar_precio_coherente(precio, modelo, anio):
        return 0
    roi = roi_info["roi"] if roi_info else calcular_roi_real(modelo, precio, anio)["roi"]
    score = 4
    if roi >= 25: score += 4
    elif roi >= 15: score += 3
    elif roi >= 10: score += 2
    elif roi >= 5: score += 1
    else: score -= 1
    if precio <= 25000: score += 2
    elif precio <= 35000: score += 1
    if len(texto.split()) >= 8: score += 1
    return max(0, min(score, 10))

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
