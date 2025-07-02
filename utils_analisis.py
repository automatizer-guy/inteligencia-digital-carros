import re
import sqlite3
from datetime import datetime, date
import os
import numpy as np

# 🚩 Ruta centralizada a la base de datos
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

ROI_MINIMO = 10.0

PRECIOS_POR_DEFECTO = {
    "yaris": 50000, "civic": 60000, "corolla": 45000, "sentra": 40000,
    "rav4": 120000, "cr-v": 90000, "tucson": 65000, "kia picanto": 39000,
    "chevrolet spark": 32000, "nissan march": 39000, "suzuki alto": 28000,
    "suzuki swift": 42000, "hyundai accent": 43000, "mitsubishi mirage": 35000,
    "suzuki grand vitara": 49000, "hyundai i10": 36000, "kia rio": 42000,
    "toyota": 45000, "honda": 47000
}

TOLERANCIA_POR_MODELO = {
    "rav4": 4, "cr-v": 3, "civic": 3, "yaris": 2, "kia picanto": 1
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

# 🧽 Limpieza robusta de enlaces para prevenir errores
def limpiar_link(link: str) -> str:
    if not link:
        return ""
    return ''.join(
        c for c in link.strip()
        if c.isascii() and c.isprintable() and c not in ['\n', '\r', '\t', '\u2028', '\u2029', '\u00A0', ' ']
    )

# 🔧 UTILIDADES DE TEXTO
def normalizar_texto(texto: str) -> str:
    return re.sub(r"[^a-z0-9]", "", texto.lower())

def coincide_modelo(titulo: str, modelo: str) -> bool:
    titulo_norm = normalizar_texto(titulo)
    for palabra in modelo.split():
        if normalizar_texto(palabra) not in titulo_norm:
            return False
    return True

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

# 📈 Estadísticas limpias de precios por modelo y año
def get_estadisticas_precio(modelo: str, año: int, return_all=False):
    tolerancia = TOLERANCIA_POR_MODELO.get(modelo, 2)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT precio FROM anuncios
        WHERE modelo = ? AND ABS(anio - ?) <= ? AND precio > 3000
    """, (modelo, año, tolerancia))
    valores = [r[0] for r in cur.fetchall()]
    conn.close()

    if not valores:
        return (PRECIOS_POR_DEFECTO.get(modelo, 0),) if not return_all else (0, 0, 0)

    q1, q3 = np.percentile(valores, [25, 75])
    iqr = q3 - q1
    filtrados = [v for v in valores if q1 - 1.5 * iqr <= v <= q3 + 1.5 * iqr]

    if not filtrados:
        filtrados = valores

    min_val = int(np.min(filtrados))
    mean_val = int(np.mean(filtrados))
    max_val = int(np.max(filtrados))
    return (min_val, mean_val, max_val) if return_all else (min_val,)

# 💰 ROI con lógica conservadora y penalización por antigüedad
def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500, return_stats=False):
    min_ref, mean_ref, max_ref = get_estadisticas_precio(modelo, año, return_all=True)
    precio_ref = min_ref
    if not precio_ref or precio_compra <= 0:
        return (0.0, {}) if return_stats else 0.0

    antiguedad = max(0, datetime.now().year - año)
    penal = max(0, antiguedad - 10) * 0.02
    precio_dep = precio_ref * (1 - penal)
    inversion = precio_compra + costo_extra
    ganancia = precio_dep - inversion
    roi = (ganancia / inversion) * 100 if inversion > 0 else 0.0
    roi = round(roi, 1)

    if return_stats:
        return roi, {
            "modelo": modelo,
            "precio_compra": precio_compra,
            "precio_ref": precio_ref,
            "penal": penal,
            "precio_dep": precio_dep,
            "ganancia": ganancia,
            "roi": roi
        }
    return roi

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
            if modelo in {"civic", "corolla", "yaris"} and r >= 20 and precio < 40000:
                pts += 2
    if contiene_negativos(txt):
        pts -= 3
    pts += 2 if 0 < precio <= 30000 else -1
    if len(tl.split()) >= 5:
        pts += 1
    return max(0, min(pts, 10))

# 📊 FUNCIONES DE BASE DE DATOS
def existe_en_db(link: str) -> bool:
    link = limpiar_link(link)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (link,))
    found = cur.fetchone() is not None
    conn.close()
    return found

def insertar_anuncio_db(url: str, modelo: str, año: int, precio: int, kilometraje: str, roi: float, score: int, relevante: bool):
    url = limpiar_link(url)
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO anuncios
            (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            url, modelo, año, precio, kilometraje,
            date.today().isoformat(), roi, score, int(relevante)
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"⚠️ Error SQLite al insertar anuncio: {e}")
    finally:
        conn.close()
