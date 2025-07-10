import re
import sqlite3
import os
import time
import atexit
from datetime import datetime, date
from typing import List, Optional, Dict, Any, Tuple

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

# Penalización por modelo (opcional)
PENAL_POR_MODEL: Dict[str, float] = {
    # "rav4": 0.015, "kia picanto": 0.03, ...
}

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
    "desarme", "chocado", "motor fundido", "no arranca", "no enciende",
    "papeles atrasados", "sin motor", "para partes", "no funciona"
]
LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# ---- Utilidades de performance ----
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

# ---- Validación de enlaces ----
def link_valido(url: str) -> bool:
    return bool(url and url.startswith("https://www.facebook.com/marketplace/item/"))

# ---- Limpieza y parsing ----
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

def extraer_anio(texto: str) -> Optional[int]:
    """
    Extrae el año de un texto (entre 1990 y 2030), usando:
    - Regex comunes y abreviadas
    - Frases semánticas frecuentes
    - Fallback fuerza bruta
    """
    texto = texto.lower()
    texto = texto.translate(str.maketrans("áéíóú", "aeiou"))
    texto_limpio = re.sub(r"[^a-z0-9\s/.-]", " ", texto)

    patrones = [
        # año completo aislado
        r"\b(19\d{2}|20[0-2]\d|2030)\b",

        # contexto con palabras clave
        r"(?:modelo|a[ñn]o|anio|version|edicion|automatico|del a[ñn]o|es|viene|trae|tiene)\D{0,6}(19\d{2}|20[0-2]\d|2030)",

        # abreviaciones comunes
        r"\bmodelo\s+(\d{2})\b",
        r"\b(?:mdl|vr|m|version)\s?(\d{2})\b",

        # fechas con guión o slash
        r"\b(0[1-9]|1[0-2])[-/](19\d{2}|20[0-2]\d|2030)\b",  # 06/2015
        r"(19\d{2}|20[0-2]\d|2030)[-/](0[1-9]|1[0-2])",      # 2015/06
    ]

    for pat in patrones:
        matches = re.findall(pat, texto_limpio)
        for match in matches:
            grupos = match if isinstance(match, tuple) else (match,)
            for grupo in grupos:
                try:
                    val = int(grupo)
                    if val < 100:
                        val += 2000  # modelo 08 → 2008
                    if 1990 <= val <= 2030:
                        return val
                except ValueError:
                    continue

    # Frases semánticas → heurística
    frases_semanticas = [
        "modelo reciente", "nuevo modelo", "del ano",
        "recien importado", "full full", "ultima generacion",
        "nuevo ingreso", "nueva version", "año actual",
        "nuevo ingreso", "recién llegado"
    ]
    for frase in frases_semanticas:
        if frase in texto_limpio:
            return datetime.now().year - 1

    # Fallback fuerza bruta
    for anio in range(2030, 1989, -1):
        if f" {anio} " in texto_limpio or f"\n{anio} " in texto_limpio or f" {anio}\n" in texto_limpio:
            return anio

    # Si DEBUG está activado, guardar texto sin año
    if os.getenv("DEBUG", "false").lower() in ("1", "true", "yes"):
        with open("anuncios_sin_anio.txt", "a", encoding="utf-8") as f:
            f.write(texto + "\n---\n")

    return None

def contar_anuncios_sin_anio(textos: List[str]) -> int:
    sin_anio = [t for t in textos if extraer_anio(t) is None]
    print(f"🔍 {len(sin_anio)} sin año de {len(textos)} textos analizados")
    return len(sin_anio)

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

# ---- Parsing completo ----
@timeit
def parsear_anuncio(texto: str) -> Optional[Tuple[str,str,int,int,str]]:
    if es_extranjero(texto) or contiene_negativos(texto):
        return None
    m_url = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", texto)
    url = limpiar_link(m_url.group()) if m_url else ""
    if not link_valido(url):
        return None
    m_pr = re.search(r"[Qq\$]\s?([\d.,]+)", texto)
    precio = limpiar_precio(m_pr.group(1)) if m_pr else 0
    if precio <= 0:
        return None
    anio = extraer_anio(texto)
    if not anio:
        return None
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    if not modelo:
        return None
    lines = [l.strip() for l in texto.splitlines() if l.strip()]
    km = lines[3] if len(lines) > 3 else ""
    return url, modelo, anio, precio, km

# ---- Score extraíble para Telegram ----
def extraer_score(texto: str) -> int:
    m = re.search(r"Score:\s?(\d{1,2})/10", texto)
    return int(m.group(1)) if m else 0

def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    parsed = parsear_anuncio(texto)
    if not parsed:
        return None
    url, modelo, anio, precio, km = parsed
    roi = calcular_roi_real(modelo, precio, anio)
    score = puntuar_anuncio(texto)
    relevante = score >= SCORE_MIN_TELEGRAM and roi >= ROI_MINIMO
    return {
        "url": url,
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "km": km,
        "roi": roi,
        "score": score,
        "relevante": relevante
    }

# ---- Cálculo de ROI ----
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
    antig = max(0, datetime.now().year - año)
    penal_por_ano = PENAL_POR_MODEL.get(modelo, PENAL_POR_ANIO)
    penal = max(0, antig - PENAL_ANIOS) * penal_por_ano
    precio_dep = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    roi = ((precio_dep - inversion) / inversion) * 100 if inversion > 0 else 0.0
    return round(roi, 1)

# ---- Scoring para DB ----
@timeit
def puntuar_anuncio(texto: str) -> int:
    parsed = parsear_anuncio(texto)
    if not parsed:
        return 0
    _, modelo, anio, precio, _ = parsed
    pts = 3
    r = calcular_roi_real(modelo, precio, anio)
    if r >= ROI_MINIMO:
        pts += 4
    elif r >= 7:
        pts += 2
    else:
        pts -= 2
    if precio <= 30000:
        pts += 2
    else:
        pts -= 1
    if len(texto.split()) >= 5:
        pts += 1
    return max(0, min(pts, 10))

# ---- Inserción en BD ----
@timeit
def insertar_anuncio_db(
    url: str, modelo: str, año: int, precio: int, km: str, roi: float, score: int, relevante: bool = False
) -> None:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO anuncios"
        " (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (url, modelo, año, precio, km, date.today().isoformat(),
         roi, score, int(score >= SCORE_MIN_DB))
    )
    conn.commit()

def existe_en_db(link: str) -> bool:
    link = limpiar_link(link)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (link,))
    found = cur.fetchone() is not None
    return found

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
    rows = cur.fetchall(); report = []
    for m, total, avg_roi, rel in rows:
        report.append(f"🚘 {m.title()}: {total} anuncios, ROI={avg_roi:.1f}%, relevantes={rel}")
    return "\n".join(report)
