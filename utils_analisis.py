import re
import sqlite3
from datetime import datetime, date
import os

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

PALABRAS_NEGATIVAS = [
    "repuesto", "repuestos", "solo repuestos", "para repuestos", "piezas",
    "desarme", "chocado", "motor fundido", "no arranca", "no enciende",
    "papeles atrasados", "sin motor", "para partes", "no funciona"
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Modo debug para loggear descartados por negativos
MODO_DEBUG = False

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
        print("🧱 Se agregó columna 'relevante' a la tabla.")
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
    resultado = any(p in low for p in PALABRAS_NEGATIVAS)
    if MODO_DEBUG and resultado:
        print(f"⚠️ DESCARTADO por palabra negativa en descripción: {repr(texto[:100])}...")
    return resultado

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

# 💰 ROI por año y modelo
def get_precio_referencia(modelo: str, año: int, tolerancia: int = 2) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(precio) FROM anuncios
        WHERE modelo = ? AND ABS(anio - ?) <= ?
    """, (modelo, año, tolerancia))
    result = cur.fetchone()
    conn.close()
    return result[0] if result and result[0] else PRECIOS_POR_DEFECTO.get(modelo, 0)

def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    precio_obj = get_precio_referencia(modelo, año)
    if not precio_obj or precio_compra <= 0:
        return 0.0
    antiguedad = max(0, datetime.now().year - año)
    penal = max(0, antiguedad - 10) * 0.02
    precio_dep = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    ganancia = precio_dep - inversion
    roi = (ganancia / inversion) * 100 if inversion > 0 else 0.0
    return round(roi, 1)

def calcular_roi(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    precio_obj = PRECIOS_POR_DEFECTO.get(modelo, 0)
    if not precio_obj or precio_compra <= 0:
        return 0.0
    antiguedad = max(0, datetime.now().year - año)
    penal = max(0, antiguedad - 10) * 0.02
    precio_dep = precio_obj * (1 - penal)
    inversion = precio_compra + costo_extra
    ganancia = precio_dep - inversion
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

# 📊 FUNCIONES DE BASE DE DATOS
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
    except sqlite3.Error as e:
        print(f"⚠️ Error SQLite al insertar anuncio: {e}")
    finally:
        conn.close()

# 📈 Función para generar resumen mensual por modelo
def resumen_mensual():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT modelo,
               COUNT(*) as total_anuncios,
               AVG(roi) as roi_promedio,
               SUM(CASE WHEN relevante=1 THEN 1 ELSE 0 END) as anuncios_relevantes
        FROM anuncios
        WHERE fecha_scrape >= date('now', '-30 days')
        GROUP BY modelo
        ORDER BY anuncios_relevantes DESC, roi_promedio DESC
    """)
    resumen = cur.fetchall()
    conn.close()

    reporte = []
    for modelo, total, roi_prom, relevantes in resumen:
        reporte.append(
            f"🚘 {modelo.title()}:\n"
            f"• Total anuncios: {total}\n"
            f"• ROI promedio últimos 30 días: {roi_prom:.1f}%\n"
            f"• Anuncios relevantes: {relevantes}\n"
        )
    return "\n".join(reporte)
