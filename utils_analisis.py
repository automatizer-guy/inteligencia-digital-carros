import os
import re
import sqlite3
import time
import unicodedata
from datetime import datetime, date
from typing import Optional, Dict, Any, List

# ---- Config ----
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 4
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 2
PENAL_ANIOS = 10
PENAL_POR_ANIO = 0.02
PENAL_POR_MODEL: Dict[str, float] = {}

PRECIOS_POR_DEFECTO = {
    "yaris": 50000, "civic": 60000, "corolla": 45000, "sentra": 40000,
    "rav4": 120000, "cr-v": 90000, "tucson": 65000, "kia picanto": 39000,
    "chevrolet spark": 32000, "nissan march": 39000, "suzuki alto": 28000,
    "suzuki swift": 42000, "hyundai accent": 43000, "mitsubishi mirage": 35000,
    "suzuki grand vitara": 49000, "hyundai i10": 36000, "kia rio": 42000,
    "toyota": 45000, "honda": 47000
}
MODELOS_INTERES = list(PRECIOS_POR_DEFECTO.keys())

PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "motor fundido", "no arranca", "no enciende", "papeles atrasados",
    "sin motor", "para partes", "no funciona"
]
LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# ---- Decorador para medir tiempo ----
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

# ---- DB ----
_conn: Optional[sqlite3.Connection] = None

def get_conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return _conn

@timeit
def inicializar_tabla_anuncios():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anuncios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link TEXT UNIQUE,
            modelo TEXT,
            anio INTEGER,
            precio INTEGER,
            km TEXT,
            fecha_scrape TEXT,
            roi REAL,
            score INTEGER,
            relevante BOOLEAN DEFAULT 0
        )
    """)
    try:
        cur.execute("ALTER TABLE anuncios ADD COLUMN relevante BOOLEAN DEFAULT 0;")
    except sqlite3.OperationalError:
        pass

# ---- Utilidades de texto ----
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
    m = re.search(r"\b\d{3,7}\b", s)
    return int(m.group()) if m else 0

# ---- Coincidencia de modelos ----
def coincide_modelo(texto: str, modelo: str) -> bool:
    texto_l = unicodedata.normalize("NFKD", texto.lower())
    modelo_l = modelo.lower()

    sinonimos = {
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

    variantes = sinonimos.get(modelo_l, []) + [modelo_l]
    texto_limpio = unicodedata.normalize("NFKD", texto_l).encode("ascii", "ignore").decode("ascii")
    return any(v in texto_limpio for v in variantes)

# ---- Año ----
def extraer_anio(texto: str) -> Optional[int]:
    texto_l = texto.lower()
    candidatos = []
    for match in re.finditer(r"\b(\d{4})\b", texto_l):
        año = int(match.group(1))
        if 1990 <= año <= datetime.now().year:
            candidatos.append(año)
    return candidatos[0] if candidatos else None

# ---- ROI y puntaje ----
@timeit
def get_precio_referencia(modelo: str, año: int, tolerancia: Optional[int] = None) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT MIN(precio) FROM anuncios WHERE modelo=? AND ABS(anio - ?) <= ?",
        (modelo, año, tolerancia or TOLERANCIA_PRECIO_REF)
    )
    base = cur.fetchone()[0] or 0
    return base or PRECIOS_POR_DEFECTO.get(modelo, 0)

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    precio_obj = get_precio_referencia(modelo, año)
    antig = max(0, datetime.now().year - año)
    penal = max(0, antig - PENAL_ANIOS) * PENAL_POR_MODEL.get(modelo, PENAL_POR_ANIO)
    precio_dep = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    roi = ((precio_dep - inversion) / inversion) * 100 if inversion > 0 else 0.0
    return round(roi, 1)

@timeit
def puntuar_anuncio(texto: str) -> int:
    score = 3
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    if not (modelo and anio and precio):
        return 0
    roi = calcular_roi_real(modelo, precio, anio)
    score += 4 if roi >= ROI_MINIMO else (2 if roi >= 7 else -2)
    score += 2 if precio <= 30000 else -1
    score += 1 if len(texto.split()) >= 5 else 0
    return max(0, min(score, 10))

# ---- DB Insert ----
@timeit
def insertar_anuncio_db(url: str, modelo: str, año: int, precio: int, km: str,
                         roi: float, score: int, relevante: bool = False):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO anuncios
        (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (url, modelo, año, precio, km, date.today().isoformat(), roi, score, int(relevante)))
    conn.commit()

def existe_en_db(link: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
    return cur.fetchone() is not None

# ---- Métricas y filtros ----
@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
        FROM anuncios WHERE modelo = ? AND fecha_scrape >= date('now', ?)
    """, (SCORE_MIN_DB, modelo, f"-{dias} days"))
    return round(cur.fetchone()[0] or 0.0, 3)

@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

# ---- Análisis ----
def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    if not (modelo and anio and precio):
        return None
    roi = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio(texto)
    url = next((l for l in texto.split() if l.startswith("http")), "")
    return {
        "url": limpiar_link(url),
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "roi": roi,
        "score": score,
        "relevante": score >= SCORE_MIN_TELEGRAM and roi >= ROI_MINIMO,
        "km": ""
    }
