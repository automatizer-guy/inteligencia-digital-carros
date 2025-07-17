import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

AÑO_ACTUAL = datetime.now().year

def escapar_multilinea(texto: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 4
SCORE_MIN_TELEGRAM = 6
ROI_MINIMO = 10.0
TOLERANCIA_PRECIO_REF = 1
DEPRECIACION_ANUAL = 0.08
MUESTRA_MINIMA_CONFIABLE = 5
MUESTRA_MINIMA_MEDIA = 2

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
    "sin motor", "para partes", "no funciona", "accidentado"
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
        if cur.fetchone() is None:
            # Crear tabla con estructura básica + columna updated_at
            cur.execute("""
                CREATE TABLE anuncios (
                    link TEXT PRIMARY KEY,
                    modelo TEXT,
                    anio INTEGER,
                    precio INTEGER,
                    km TEXT,
                    fecha_scrape DATE,
                    roi REAL,
                    score INTEGER,
                    updated_at TEXT DEFAULT CURRENT_DATE

                )
            """)
            print("✅ Tabla anuncios creada con updated_at")
        # Agregar updated_at si no existe
        cur.execute("PRAGMA table_info(anuncios)")
        cols = {row[1] for row in cur.fetchall()}
        if 'updated_at' not in cols:
            try:
                cur.execute("ALTER TABLE anuncios ADD COLUMN updated_at DATE DEFAULT DATE('now')")
                print("✅ Columna 'updated_at' agregada")
            except sqlite3.OperationalError:
                pass

        # Verificar y agregar otras columnas
        # ... tu lógica de relevancia, confianza_precio, muestra_precio...

        conn.commit()
```

```python
# Nueva función UPSERT en lugar de insertar_anuncio_db

def insertar_o_actualizar_anuncio_db(
    conn,
    link: str,
    modelo: str,
    anio: int,
    precio: int,
    km: str,
    roi: float,
    score: int,
    relevante: bool,
    confianza_precio: str,
    muestra_precio: int
) -> str:
    """
    Inserta o actualiza un anuncio y devuelve 'nuevo' o 'actualizado'.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO anuncios (
          link, modelo, anio, precio, km, roi, score,
          relevante, confianza_precio, muestra_precio,
          fecha_scrape, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'), DATE('now'))
        ON CONFLICT(link) DO UPDATE SET
          modelo=excluded.modelo,
          anio=excluded.anio,
          precio=excluded.precio,
          km=excluded.km,
          roi=excluded.roi,
          score=excluded.score,
          relevante=excluded.relevante,
          confianza_precio=excluded.confianza_precio,
          muestra_precio=excluded.muestra_precio,
          updated_at=DATE('now')
    """, (
        link, modelo, anio, precio, km, roi, score,
        int(relevante), confianza_precio, muestra_precio
    ))
    conn.commit()
    return "nuevo" if cur.lastrowid else "actualizado"


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
    candidatos = [int(x) for x in matches if int(x) >= 3000 and (int(x) < 1990 or int(x) > AÑO_ACTUAL + 1)]
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

def extraer_anio(texto: str, anio_actual: int = None) -> Optional[int]:
    if anio_actual is None:
        anio_actual = datetime.now().year

    texto = texto.lower()
    
    # 1. Detectar años de 2 dígitos tipo "modelo 98"
    match_modelo = re.search(r"(modelo|año)\s?(\d{2})\b", texto)
    if match_modelo:
        anio = int(match_modelo.group(2))
        if anio >= 90:
            return 1900 + anio  # ejemplo: 98 → 1998
        else:
            return 2000 + anio  # ejemplo: 05 → 2005

    # 2. Detectar años de 4 dígitos que NO estén en frases irrelevantes
    # Frases a ignorar
    patrones_ignorar = [
        r"se unió a facebook en \d{4}",
        r"miembro desde \d{4}",
        r"en facebook desde \d{4}",
        r"perfil creado en \d{4}",
    ]
    for patron in patrones_ignorar:
        texto = re.sub(patron, '', texto)

    # 3. Extraer años válidos
    posibles = re.findall(r"\b(19\d{2}|20[0-3]\d)\b", texto)
    for anio in posibles:
        anio_int = int(anio)
        if 1990 <= anio_int <= anio_actual:
            return anio_int

    return None

def validar_coherencia_precio_año(precio: int, anio: int) -> bool:
    if anio >= 2020 and precio < 100_000:
        return False
    if anio >= 2016 and precio < 50_000:
        return False
    if anio >= 2010 and precio < 30_000:
        return False
    return True

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
    años_ant = max(0, AÑO_ACTUAL - anio)
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
    cur.execute("PRAGMA table_info(anuncios)")
    columnas_existentes = {row[1] for row in cur.fetchall()}

    if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]):
        cur.execute("""
        INSERT OR REPLACE INTO anuncios 
        (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio))
    else:
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
