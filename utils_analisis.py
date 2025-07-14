import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

def escapar_multilinea(texto: str) -> str:
    """Escapa caracteres especiales según formato MarkdownV2 de Telegram."""
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

# ---- Config ----
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 4
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 1  # Reducido de 2 a 1 año
DEPRECIACION_ANUAL = 0.08  # 8% anual más realista
MUESTRA_MINIMA_CONFIABLE = 5
MUESTRA_MINIMA_MEDIA = 2

# Valores más conservadores y realistas
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
    "sin motor", "para partes", "no funciona", "chocado", "accidentado"
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

# ---- DB Connection Manager ----
@contextmanager
def get_db_connection():
    """Context manager para manejo seguro de conexiones DB"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()

# Mantener conexión global para compatibilidad
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
                relevante BOOLEAN DEFAULT 0,
                confianza_precio TEXT DEFAULT 'baja',
                muestra_precio INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Agregar nuevas columnas si no existen
        for columna in [
            "relevante BOOLEAN DEFAULT 0",
            "confianza_precio TEXT DEFAULT 'baja'",
            "muestra_precio INTEGER DEFAULT 0",
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ]:
            try:
                cur.execute(f"ALTER TABLE anuncios ADD COLUMN {columna};")
            except sqlite3.OperationalError:
                pass
        
        conn.commit()

# ---- Utilidades de texto ----
def limpiar_link(link: Optional[str]) -> str:
    if not link:
        return ""
    return ''.join(c for c in link.strip() if c.isascii() and c.isprintable())

def contiene_negativos(texto: str) -> bool:
    return any(p in texto.lower() for p in PALABRAS_NEGATIVAS)

def es_extranjero(texto: str) -> bool:
    return any(p in texto.lower() for p in LUGARES_EXTRANJEROS)

def validar_precio_coherente(precio: int, modelo: str, año: int) -> bool:
    """Valida que el precio esté dentro de rangos coherentes"""
    if precio < 5000 or precio > 500000:
        return False
    
    precio_ref = PRECIOS_POR_DEFECTO.get(modelo, 50000)
    # Permitir variación de 0.2x a 2.5x del precio de referencia
    return 0.2 * precio_ref <= precio <= 2.5 * precio_ref

def limpiar_precio(texto: str) -> int:
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{3,7}\b", s)
    año_actual = datetime.now().year
    candidatos = [int(x) for x in matches if int(x) < 1990 or int(x) > año_actual + 1]
    return candidatos[0] if candidatos else 0

def filtrar_outliers(precios: List[int]) -> List[int]:
    """Elimina precios extremos usando método IQR"""
    if len(precios) < 4:
        return precios
    
    try:
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        limite_inferior = q1 - 1.5 * iqr
        limite_superior = q3 + 1.5 * iqr
        
        filtrados = [p for p in precios if limite_inferior <= p <= limite_superior]
        return filtrados if len(filtrados) >= 2 else precios
    except:
        return precios

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
    
    # Buscar patrones específicos primero
    patterns = [
        r"año\s*(\d{4})",
        r"modelo\s*(\d{4})",
        r"del\s*(\d{4})",
        r"(\d{4})\s*model"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, texto_l)
        for match in matches:
            año = int(match)
            if 1990 <= año <= datetime.now().year:
                candidatos.append(año)
    
    # Si no encuentra nada específico, buscar cualquier año válido
    if not candidatos:
        for match in re.finditer(r"\b(\d{4})\b", texto_l):
            año = int(match.group(1))
            if 1990 <= año <= datetime.now().year:
                candidatos.append(año)
    
    return candidatos[0] if candidatos else None

# ---- Precio de referencia mejorado ----
@timeit
def get_precio_referencia(modelo: str, año: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """
    Obtiene precio de referencia usando mediana y filtrado de outliers
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT precio FROM anuncios 
            WHERE modelo=? AND ABS(anio - ?) <= ? AND precio > 0
            ORDER BY precio
        """, (modelo, año, tolerancia or TOLERANCIA_PRECIO_REF))
        
        precios = [row[0] for row in cur.fetchall()]
    
    if len(precios) >= MUESTRA_MINIMA_CONFIABLE:
        precios_filtrados = filtrar_outliers(precios)
        mediana = statistics.median(precios_filtrados)
        return {
            "precio": int(mediana),
            "confianza": "alta",
            "muestra": len(precios_filtrados),
            "rango": f"{min(precios_filtrados)}-{max(precios_filtrados)}"
        }
    elif len(precios) >= MUESTRA_MINIMA_MEDIA:
        mediana = statistics.median(precios)
        return {
            "precio": int(mediana),
            "confianza": "media",
            "muestra": len(precios),
            "rango": f"{min(precios)}-{max(precios)}"
        }
    else:
        return {
            "precio": PRECIOS_POR_DEFECTO.get(modelo, 50000),
            "confianza": "baja",
            "muestra": 0,
            "rango": "default"
        }

# ---- ROI y puntaje mejorados ----
@timeit
def calcular_roi_real(modelo: str, precio_compra: int, año: int, 
                      costo_extra: int = 2000) -> Dict[str, Any]:
    """
    Calcula ROI más realista con información de confianza
    """
    ref_data = get_precio_referencia(modelo, año)
    
    # Calcular depreciación más realista
    años_antiguedad = max(0, datetime.now().year - año)
    factor_depreciacion = (1 - DEPRECIACION_ANUAL) ** años_antiguedad
    precio_depreciado = ref_data["precio"] * factor_depreciacion
    
    inversion_total = precio_compra + costo_extra
    roi = ((precio_depreciado - inversion_total) / inversion_total) * 100 if inversion_total > 0 else 0.0
    
    return {
        "roi": round(roi, 1),
        "precio_referencia": ref_data["precio"],
        "precio_depreciado": int(precio_depreciado),
        "confianza": ref_data["confianza"],
        "muestra": ref_data["muestra"],
        "inversion_total": inversion_total,
        "años_antiguedad": años_antiguedad
    }

@timeit
def puntuar_anuncio(texto: str, roi_info: Optional[Dict] = None) -> int:
    """
    Puntúa anuncio de forma más sistemática
    """
    precio = limpiar_precio(texto)
    anio = extraer_anio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    
    if not (modelo and anio and precio):
        return 0
    
    # Validar coherencia del precio
    if not validar_precio_coherente(precio, modelo, anio):
        return 0
    
    # Usar ROI precalculado o calcularlo
    if roi_info:
        roi = roi_info["roi"]
        confianza = roi_info["confianza"]
    else:
        roi_data = calcular_roi_real(modelo, precio, anio)
        roi = roi_data["roi"]
        confianza = roi_data["confianza"]
    
    # Score base
    score = 5
    
    # Ajustes por ROI
    if roi >= 25:
        score += 3
    elif roi >= 15:
        score += 2
    elif roi >= 10:
        score += 1
    elif roi >= 5:
        score += 0
    else:
        score -= 2
    
    # Ajustes por confianza del precio
    if confianza == "alta":
        score += 1
    elif confianza == "baja":
        score -= 1
    
    # Ajustes por precio
    if precio <= 25000:
        score += 1
    elif precio >= 80000:
        score -= 1
    
    # Ajustes por completitud del anuncio
    if len(texto.split()) >= 8:
        score += 1
    
    return max(0, min(score, 10))

# ---- DB Insert mejorado ----
@timeit
def insertar_anuncio_db(url: str, modelo: str, año: int, precio: int, km: str,
                         roi: float, score: int, relevante: bool = False,
                         confianza_precio: str = "baja", muestra_precio: int = 0):
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO anuncios
            (link, modelo, anio, precio, km, fecha_scrape, roi, score, relevante, 
             confianza_precio, muestra_precio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (url, modelo, año, precio, km, date.today().isoformat(), 
              roi, score, int(relevante), confianza_precio, muestra_precio))
        conn.commit()

def existe_en_db(link: str) -> bool:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
        return cur.fetchone() is not None

# ---- Métricas y filtros mejorados ----
@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
            FROM anuncios WHERE modelo = ? AND fecha_scrape >= date('now', ?)
        """, (SCORE_MIN_DB, modelo, f"-{dias} days"))
        return round(cur.fetchone()[0] or 0.0, 3)

@timeit
def modelos_bajo_rendimiento(threshold: float = 0.005, dias: int = 7) -> List[str]:
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

def get_estadisticas_db() -> Dict[str, Any]:
    """Obtiene estadísticas útiles de la base de datos"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Estadísticas básicas
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'alta'")
        alta_confianza = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'baja'")
        baja_confianza = cur.fetchone()[0]
        
        cur.execute("""
            SELECT modelo, COUNT(*) as count 
            FROM anuncios 
            GROUP BY modelo 
            ORDER BY count DESC
        """)
        por_modelo = dict(cur.fetchall())
        
        return {
            "total_anuncios": total,
            "alta_confianza": alta_confianza,
            "baja_confianza": baja_confianza,
            "porcentaje_defaults": round((baja_confianza / total) * 100, 1) if total > 0 else 0,
            "por_modelo": por_modelo
        }

# ---- Análisis mejorado ----
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
        "url": limpiar_link(url),
        "modelo": modelo,
        "año": anio,
        "precio": precio,
        "roi": roi_data["roi"],
        "score": score,
        "relevante": score >= SCORE_MIN_TELEGRAM and roi_data["roi"] >= ROI_MINIMO,
        "km": "",
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data
    }
