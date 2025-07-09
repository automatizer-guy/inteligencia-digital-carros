import re
import sqlite3
import os
from datetime import datetime, date
from typing import List, Optional

# 🚩 Ruta absoluta a la base de datos (asegura crear la carpeta)
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# 🔧 Parámetros globales configurables
SCORE_MIN_DB = 4
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 2

# Precio de referencia base para ROI, ajustable por modelo
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

# ---- Funciones ----

def inicializar_tabla_anuncios():
    """Crea la tabla anuncios con esquema si no existe, maneja columna 'relevante'."""
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
    # Intentar agregar columna 'relevante' si no existe (SQLite no soporta IF NOT EXISTS en ALTER)
    try:
        cur.execute("ALTER TABLE anuncios ADD COLUMN relevante BOOLEAN DEFAULT 0;")
    except sqlite3.OperationalError:
        # La columna ya existe
        pass
    conn.commit()
    conn.close()

def limpiar_link(link: Optional[str]) -> str:
    """Quita caracteres invisibles, espacios y normaliza el link."""
    if not link:
        return ""
    return ''.join(
        c for c in link.strip()
        if c.isascii() and c.isprintable() and c not in ['\n', '\r', '\t', '\u2028', '\u2029', '\u00A0', ' ']
    )

def normalizar_texto(texto: str) -> str:
    """Convierte a minúsculas y elimina todo excepto letras y números para comparación."""
    return re.sub(r"[^a-z0-9]", "", texto.lower())

def coincide_modelo(titulo: str, modelo: str) -> bool:
    """Verifica que todas las palabras del modelo estén en el título."""
    titulo_norm = normalizar_texto(titulo)
    for palabra in modelo.split():
        if normalizar_texto(palabra) not in titulo_norm:
            return False
    return True

def limpiar_precio(texto: str) -> int:
    """Extrae un entero válido de precio de un string."""
    s = texto.lower().replace("q", "").replace("$", "").replace("mx", "") \
                     .replace(".", "").replace(",", "").strip()
    m = re.search(r"\b\d{3,6}\b", s)
    return int(m.group()) if m else 0

def contiene_negativos(texto: str) -> bool:
    """Detecta si el texto contiene alguna palabra negativa."""
    texto = texto.lower()
    return any(p in texto for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    """Detecta si el texto menciona lugares extranjeros."""
    texto = texto.lower()
    return any(p in texto for p in LUGARES_EXTRANJEROS)

def extraer_anio(texto: str) -> Optional[int]:
    """Extrae un año válido entre 1990 y 2030 del texto."""
    m = re.search(r"(19|20)\d{2}", texto)
    anio = int(m.group()) if m else None
    return anio if 1990 <= (anio or 0) <= 2030 else None

def get_precio_referencia(modelo: str, año: int, tolerancia: int = None) -> int:
    """Obtiene el precio mínimo histórico para modelo y año ± tolerancia."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(precio) FROM anuncios
        WHERE modelo = ? AND ABS(anio - ?) <= ?
    """, (modelo, año, tolerancia or TOLERANCIA_PRECIO_REF))
    result = cur.fetchone()
    conn.close()
    return result[0] if result and result[0] else PRECIOS_POR_DEFECTO.get(modelo, 0)

def calcular_roi_real(modelo: str, precio_compra: int, año: int, costo_extra: int = 1500) -> float:
    """
    Calcula ROI % conservador, penalizando antigüedad > 10 años.
    """
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

def puntuar_anuncio(titulo: str, precio: int, texto: Optional[str] = None) -> int:
    """
    Score simple basado en presencia de modelo, ROI, palabras negativas y características básicas.
    """
    texto = (texto or titulo).lower()
    pts = 0
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(titulo, m)), None)
    if modelo:
        pts += 3
        anio = extraer_anio(texto)
        if anio:
            r = calcular_roi_real(modelo, precio, anio)
            if r >= ROI_MINIMO:
                pts += 4
            elif r >= 7:
                pts += 2
            else:
                pts -= 2
    if contiene_negativos(texto):
        pts -= 3
    if 0 < precio <= 30000:
        pts += 2
    else:
        pts -= 1
    if len(titulo.split()) >= 5:
        pts += 1
    return max(0, min(pts, 10))

def insertar_anuncio_db(
    url: str, modelo: str, año: int, precio: int, km: str, roi: float, score: int, relevante: bool
):
    """Inserta anuncio en DB ignorando duplicados y maneja errores."""
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
            km,
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

def existe_en_db(link: str) -> bool:
    """Verifica si un link ya existe en la DB."""
    link = limpiar_link(link)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (link,))
    found = cur.fetchone() is not None
    conn.close()
    return found

def analizar_mensaje(texto: str) -> Optional[dict]:
    """
    Extrae datos de un mensaje de Telegram en formato esperado,
    devuelve dict con info si es válido, None si no.
    """
    url_match = re.search(r"https://www\.facebook\.com/marketplace/item/\d+", texto)
    precio_match = re.search(r"Precio: Q([\d,\.]+)", texto)
    modelo_match = re.search(r"🚘 \*(.+?)\*", texto)
    anio = extraer_anio(texto)

    url = limpiar_link(url_match.group()) if url_match else ""
    precio = limpiar_precio(precio_match.group(1)) if precio_match else 0
    modelo_txt = modelo_match.group(1).lower() if modelo_match else ""

    if not all([url, precio, anio, modelo_txt]):
        return None
    if es_extranjero(texto) or contiene_negativos(texto):
        return None

    detectado = next((m for m in MODELOS_INTERES if coincide_modelo(modelo_txt, m)), None)
    if not detectado:
        return None

    roi = calcular_roi_real(detectado, precio, anio)
    score = puntuar_anuncio(modelo_txt, precio, texto)

    return {
        "url": url,
        "modelo": detectado,
        "año": anio,
        "precio": precio,
        "roi": roi,
        "score": score,
        "relevante": score >= SCORE_MIN_TELEGRAM
    }

def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    """
    Porcentaje de anuncios con score >= SCORE_MIN_DB para un modelo en últimos 'dias' días.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END)*1.0 / COUNT(*)
        FROM anuncios
        WHERE modelo = ?
          AND fecha_scrape >= date('now', ?)
    """, (SCORE_MIN_DB, modelo, f"-{dias} days"))
    ratio = cur.fetchone()[0] or 0.0
    conn.close()
    return round(ratio, 3)

def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    """
    Retorna lista de modelos con rendimiento bajo en últimos 'dias' días según threshold.
    """
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

def resumen_mensual() -> str:
    """
    Retorna reporte con conteo, ROI promedio y anuncios relevantes en últimos 30 días por modelo.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT modelo,
               COUNT(*) as total_anuncios,
               AVG(roi) as roi_promedio,
               SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) as anuncios_relevantes
        FROM anuncios
        WHERE fecha_scrape >= date('now', '-30 days')
        GROUP BY modelo
        ORDER BY anuncios_relevantes DESC, roi_promedio DESC
    """, (SCORE_MIN_DB,))
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
