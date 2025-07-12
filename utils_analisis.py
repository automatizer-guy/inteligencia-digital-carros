import re
import sqlite3
import os
import time
import atexit
from collections import Counter
from datetime import datetime, date
from typing import List, Optional, Dict, Any, Tuple

# Ruta a la base de datos
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Parámetros globales
DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 4
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 2
PENAL_ANIOS = 10
PENAL_POR_ANIO = 0.02

PENAL_POR_MODEL: Dict[str, float] = {}

PRECIOS_POR_DEFECTO: Dict[str, int] = {
    "yaris": 50000, "civic": 60000, "corolla": 45000, "sentra": 40000,
    "rav4": 120000, "cr-v": 90000, "tucson": 65000, "kia picanto": 39000,
    "chevrolet spark": 32000, "nissan march": 39000, "suzuki alto": 28000,
    "suzuki swift": 42000, "hyundai accent": 43000, "mitsubishi mirage": 35000,
    "suzuki grand vitara": 49000, "hyundai i10": 36000, "kia rio": 42000,
    "toyota": 45000, "honda": 47000
}
MODELOS_INTERES: List[str] = list(PRECIOS_POR_DEFECTO.keys())

PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "motor fundido", "no arranca", "no enciende",
    "papeles atrasados", "sin motor", "para partes", "no funciona"
]
LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

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

def limpiar_link(link: Optional[str]) -> str:
    if not link:
        return ""
    return ''.join(c for c in link.strip()
                   if c.isascii() and c.isprintable()
                   and c not in ['\n','\r','\t','\u2028','\u2029','\u00A0',' '])

def texto_a_numero(texto: str) -> Optional[int]:
    mapa = {
        'uno': 1, 'dos': 2, 'tres': 3, 'cuatro': 4, 'cinco': 5,
        'seis': 6, 'siete': 7, 'ocho': 8, 'nueve': 9, 'diez': 10,
        'once': 11, 'doce': 12, 'trece': 13, 'catorce': 14, 'quince': 15,
        'dieciseis': 16, 'diecisiete': 17, 'dieciocho': 18, 'diecinueve': 19,
        'veinte': 20, 'veintiuno': 21, 'veintidos': 22, 'veintitres': 23,
        'veinticuatro': 24
    }
    texto = texto.lower()
    for palabra, valor in mapa.items():
        if f"dos mil {palabra}" in texto:
            return 2000 + valor
    return None

def extraer_anio(texto: str) -> Optional[int]:
    texto_l = texto.lower()
    patrones = [r"\b(?:modelo|año|ano|del)?\s*(\d{4})\b"]
    for patron in patrones:
        for match in re.finditer(patron, texto_l):
            try:
                año = int(match.group(1))
                if 1990 <= año <= datetime.now().year + 1:
                    return año
            except:
                continue
    return texto_a_numero(texto_l)

def contiene_negativos(texto: str) -> bool:
    return any(p in texto.lower() for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    m = re.search(r"\b\d{3,7}\b", s)
    return int(m.group()) if m else 0

@timeit
def get_precio_referencia(modelo: str, año: int, tolerancia: Optional[int] = None) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT MIN(precio) FROM anuncios WHERE modelo=? AND ABS(anio - ?) <= ?",
        (modelo, año, tolerancia or TOLERANCIA_PRECIO_REF)
    )
    base = cur.fetchone()[0] or 0
    return max(base, PRECIOS_POR_DEFECTO.get(modelo, 0))

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    precio_obj = get_precio_referencia(modelo, año)
    antig = max(0, datetime.now().year - año)
    penal_por_ano = PENAL_POR_MODEL.get(modelo, PENAL_POR_ANIO)
    penal = max(0, antig - PENAL_ANIOS) * penal_por_ano
    precio_dep = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    roi = ((precio_dep - inversion) / inversion) * 100 if inversion > 0 else 0.0
    return round(roi, 1)

@timeit
def puntuar_anuncio(texto: str) -> int:
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if m in texto.lower()), None)
    if not (modelo and anio and precio):
        return 0
    pts = 3
    roi = calcular_roi_real(modelo, precio, anio)
    if roi >= ROI_MINIMO:
        pts += 4
    elif roi >= 7:
        pts += 2
    else:
        pts -= 2
    if precio <= 30000:
        pts += 2
    if len(texto.split()) >= 5:
        pts += 1
    return max(0, min(pts, 10))

@timeit
def insertar_anuncio_db(url: str, modelo: str, año: int, precio: int, km: str, roi: float, score: int, relevante: bool = False) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO anuncios (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (url, modelo, año, precio, km, date.today().isoformat(), roi, score, int(relevante))
    )
    conn.commit()

def existe_en_db(link: str) -> bool:
    link = limpiar_link(link)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (link,))
    return cur.fetchone() is not None

@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0 / COUNT(*) "
        "FROM anuncios WHERE modelo = ? AND fecha_scrape >= date('now', ?)",
        (SCORE_MIN_DB, modelo, f"-{dias} days")
    )
    return round(cur.fetchone()[0] or 0.0, 3)

@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

@timeit
def resumen_mensual() -> str:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT modelo, COUNT(*) total, AVG(roi) avg_roi, "
        "SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) rel "
        "FROM anuncios WHERE fecha_scrape >= date('now', '-30 days') "
        "GROUP BY modelo ORDER BY rel DESC, avg_roi DESC",
        (SCORE_MIN_DB,)
    )
    rows = cur.fetchall()
    report = []
    for m, total, avg_roi, rel in rows:
        report.append(f"🚘 {m.title()}: {total} anuncios, ROI={avg_roi:.1f}%, relevantes={rel}")
    return "\n".join(report)
