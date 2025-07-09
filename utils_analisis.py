import re
import sqlite3
import os
import time
import atexit
from datetime import datetime, date
from typing import List, Optional, Callable, Any, Tuple

# 🚩 Ruta a la base de datos
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# 🔧 Parámetros globales
DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 4
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 2
PENAL_ANIOS = 10
PENAL_POR_ANIO = 0.02
PENAL_POR_MODEL = {
    # Ejemplo: "rav4": 0.015,
}

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
    "desarme", "chocado", "motor fundido", "no arranca", "no enciende",
    "papeles atrasados", "sin motor", "para partes", "no funciona"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# ---- Decorator de performance ----
def timeit(func: Callable) -> Callable:
    def wrapper(*args, **kwargs) -> Any:
        if not DEBUG:
            return func(*args, **kwargs)
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"⌛ {func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper

# ---- Conexión SQLite compartida ----
_conn: Optional[sqlite3.Connection] = None
def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return _conn

@atexit.register
def _close_conn():
    global _conn
    if _conn:
        _conn.close()
        _conn = None

# ---- Inicialización de la tabla ----
@timeit
def inicializar_tabla_anuncios() -> None:
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
        );
    """)
    try:
        cur.execute("ALTER TABLE anuncios ADD COLUMN relevante BOOLEAN DEFAULT 0;")
    except sqlite3.OperationalError:
        pass

# ---- Limpieza y validación ----
def limpiar_link(link: Optional[str]) -> str:
    if not link:
        return ""
    return ''.join(c for c in link.strip()
                   if c.isascii() and c.isprintable()
                   and c not in ['\n','\r','\t','\u2028','\u2029','\u00A0',' '])

def normalizar_texto(texto: str) -> str:
    return re.sub(r"[^a-z0-9]", "", texto.lower())

def coincide_modelo(titulo: str, modelo: str) -> bool:
    norm = normalizar_texto(titulo)
    return all(normalizar_texto(p) in norm for p in modelo.split())

def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    m = re.search(r"\b\d{3,7}\b", s)
    return int(m.group()) if m else 0

def contiene_negativos(texto: str) -> bool:
    low = texto.lower()
    return any(p in low for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    low = texto.lower()
    return any(p in low for p in LUGARES_EXTRANJEROS)

# ---- Extracción de año ----
def extraer_anio(texto: str) -> Optional[int]:
    for pat in [r"\b(19\d{2}|20[0-2]\d|2030)\b",
                r"[-•]\s*(19\d{2}|20[0-2]\d)\s*[-•]",
                r"(19\d{2}|20[0-2]\d)[,\.]"]:
        m = re.search(pat, texto)
        if m:
            an = int(m.group(1))
            if 1990 <= an <= 2030:
                return an
    for line in texto.splitlines():
        m = re.match(r"^(19\d{2}|20[0-2]\d|2030)\b", line.strip())
        if m:
            an = int(m.group(1))
            if 1990 <= an <= 2030:
                return an
    return None

# ---- Funciones de base de datos ----
def existe_en_db(link: str) -> bool:
    link = limpiar_link(link)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (link,))
    return cur.fetchone() is not None

def insertar_anuncio_db(
    url: str, modelo: str, año: int, precio: int, km: str, roi: float, score: int, relevante: bool
) -> None:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO anuncios (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (url, modelo, año, precio, km, date.today().isoformat(), roi, score, int(bool(relevante)))
    )
    conn.commit()

# ---- ROI y scoring ----
@timeit
def get_precio_referencia(modelo: str, año: int, tolerancia: Optional[int] = None) -> int:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT MIN(precio) FROM anuncios WHERE modelo=? AND ABS(anio-?)<=?",
        (modelo, año, tolerancia or TOLERANCIA_PRECIO_REF)
    )
    base = cur.fetchone()[0] or 0
    return base or PRECIOS_POR_DEFECTO.get(modelo, 0)

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    precio_obj = get_precio_referencia(modelo, año)
    if precio_obj <= 0 or precio_compra <= 0:
        return 0.0
    antig = max(0, datetime.now().year - año)
    penal_por_ano = PENAL_POR_MODEL.get(modelo, PENAL_POR_ANIO)
    penal = max(0, antig - PENAL_ANIOS) * penal_por_ano
    precio_dep = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    roi = ((precio_dep - inversion) / inversion) * 100 if inversion > 0 else 0
    return round(roi, 1)

def puntuar_anuncio(titulo: str, precio: int, texto: Optional[str] = None) -> int:
    txt = (texto or titulo).lower()
    pts = 0
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(titulo, m)), None)
    if modelo:
        pts += 3
        anio = extraer_anio(txt)
        if anio is not None:
            r = calcular_roi_real(modelo, precio, anio)
            pts += 4 if r >= ROI_MINIMO else 2 if r >= 7 else -2
    if contiene_negativos(txt):
        pts -= 3
    pts += 2 if precio <= 30000 and precio > 0 else -1
    pts += 1 if len(txt.split()) >= 5 else 0
    return max(0, min(pts, 10))

# ---- Métricas históricas ----
@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT SUM(CASE WHEN score>=? THEN 1 ELSE 0 END)*1.0/COUNT(*) "
        "FROM anuncios WHERE modelo=? AND fecha_scrape>=date('now',?)",
        (SCORE_MIN_DB, modelo, f"-{dias} days")
    )
    return round(cur.fetchone()[0] or 0.0, 3)

@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

# ---- Resumen mensual ----
@timeit
def resumen_mensual() -> str:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT modelo, COUNT(*) total, AVG(roi) avg_roi, "
        "SUM(CASE WHEN score>=? THEN 1 ELSE 0 END) rel "
        "FROM anuncios WHERE fecha_scrape>=date('now','-30 days') "
        "GROUP BY modelo ORDER BY rel DESC, avg_roi DESC",
        (SCORE_MIN_DB,)
    )
    report = []
    for m, total, avg_roi, rel in cur.fetchall():
        report.append(f"🚘 {m.title()}: {total} anuncios, ROI={avg_roi:.1f}%, relevantes={rel}")
    return "\n".join(report)
