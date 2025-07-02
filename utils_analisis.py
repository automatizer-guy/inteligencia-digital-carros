import re
import sqlite3
from datetime import datetime, date
import os
import statistics

# 🚩 Ruta centralizada a la base de datos
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

ROI_MINIMO = 10.0
COSTO_EXTRA_FIJO = 1500

# 💰 Valores por defecto si no hay datos suficientes
PRECIOS_POR_DEFECTO = {
    "yaris": 50000, "civic": 60000, "corolla": 45000, "sentra": 40000,
    "rav4": 120000, "cr-v": 90000, "tucson": 65000, "kia picanto": 39000,
    "chevrolet spark": 32000, "nissan march": 39000, "suzuki alto": 28000,
    "suzuki swift": 42000, "hyundai accent": 43000, "mitsubishi mirage": 35000,
    "suzuki grand vitara": 49000, "hyundai i10": 36000, "kia rio": 42000,
    "toyota": 45000, "honda": 47000
}

PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "chocado", "motor fundido", "no arranca", "no enciende",
    "papeles atrasados", "sin motor", "para partes", "no funciona"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# 🔧 Crear tabla si no existe
def inicializar_tabla_anuncios():
    conn = sqlite3.connect(DB_PATH)
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
    conn.commit()
    conn.close()

# 🔗 Limpieza robusta de enlaces
def limpiar_link(link: str) -> str:
    if not link:
        return ""
    return ''.join(
        c for c in link.strip()
        if c.isascii() and c.isprintable() and c not in ['\n', '\r', '\t', '\u2028', '\u2029', '\u00A0', ' ']
    )

# 🧹 Limpieza y normalización de texto
def normalizar_texto(texto: str) -> str:
    return re.sub(r"[^a-z0-9]", "", texto.lower())

def coincide_modelo(titulo: str, modelo: str) -> bool:
    titulo_norm = normalizar_texto(titulo)
    return all(normalizar_texto(p) in titulo_norm for p in modelo.split())

def limpiar_precio(texto: str) -> int:
    s = texto.lower().replace("q", "").replace("$", "").replace("mx", "") \
                   .replace(".", "").replace(",", "").strip()
    m = re.search(r"\b\d{3,6}\b", s)
    return int(m.group()) if m else 0

def contiene_negativos(texto: str) -> bool:
    low = texto.lower()
    return any(p in low for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

# 📊 Función mejorada para obtener estadísticas históricas
def get_estadisticas_precio(modelo: str, año: int, tolerancia: int = 2):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT precio FROM anuncios
        WHERE modelo = ? AND ABS(anio - ?) <= ? AND precio > 3000
    """, (modelo, año, tolerancia))
    precios = [r[0] for r in cur.fetchall() if r[0] is not None]
    conn.close()
    if len(precios) >= 3:
        return {
            "min": min(precios),
            "max": max(precios),
            "avg": round(statistics.mean(precios)),
            "mediana": round(statistics.median(precios)),
            "p25": round(statistics.quantiles(precios, n=4)[0]),
            "p75": round(statistics.quantiles(precios, n=4)[2])
        }
    else:
        default = PRECIOS_POR_DEFECTO.get(modelo, 0)
        return {"min": default, "max": default, "avg": default, "mediana": default, "p25": default, "p75": default}

# 💰 Cálculo mejorado de ROI
def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = COSTO_EXTRA_FIJO) -> float:
    stats = get_estadisticas_precio(modelo, año)
    precio_obj = stats["avg"]
    antiguedad = max(0, datetime.now().year - año)
    penal = max(0, antiguedad - 10) * 0.015
    precio_ajustado = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    ganancia = precio_ajustado - inversion
    roi = (ganancia / inversion) * 100 if inversion > 0 else 0.0
    return round(roi, 1)

def puntuar_anuncio(titulo: str, precio: int, texto: str = None) -> int:
    tl = titulo.lower()
    txt = (texto or tl).lower()
    pts = 0
    modelo = next((m for m in PRECIOS_POR_DEFECTO if coincide_modelo(titulo, m)), None)
    if modelo:
        pts += 3
        match = re.search(r"\b(19|20)\d{2}\b", txt)
        año = int(match.group()) if match else None
        if año:
            r = calcular_roi_real(modelo, precio, año)
            pts += 4 if r >= ROI_MINIMO else 2 if r >= 7 else -2
    if contiene_negativos(txt):
        pts -= 3
    pts += 2 if 0 < precio <= 30000 else -1
    if len(tl.split()) >= 5:
        pts += 1
    return max(0, min(pts, 10))

# 📦 FUNCIONES BASE DE DATOS
def existe_en_db(link: str) -> bool:
    link = limpiar_link(link)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (link,))
    found = cur.fetchone() is not None
    conn.close()
    return found

def insertar_anuncio_db(
    url: str,
    modelo: str,
    año: int,
    precio: int,
    kilometraje: str,
    roi: float,
    score: int,
    relevante: bool
):
    url = limpiar_link(url)
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO anuncios
            (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            url,
            modelo,
            año,
            precio,
            kilometraje,
            date.today().isoformat(),
            roi,
            score,
            int(relevante)
        ))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Error al insertar anuncio: {e}")
    finally:
        conn.close()
