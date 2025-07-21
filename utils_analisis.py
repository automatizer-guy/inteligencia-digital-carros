import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

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
        tabla_existe = cur.fetchone() is not None
        
        if not tabla_existe:
            # Crear tabla con estructura básica
            cur.execute("""
                CREATE TABLE anuncios (
                    link TEXT PRIMARY KEY,
                    modelo TEXT,
                    anio INTEGER,
                    precio INTEGER,
                    km TEXT,
                    fecha_scrape DATE,
                    roi REAL,
                    score INTEGER
                )
            """)
            print("✅ Tabla anuncios creada con estructura básica")
        
        # Verificar columnas existentes
        cur.execute("PRAGMA table_info(anuncios)")
        columnas_existentes = {row[1] for row in cur.fetchall()}
        
        # Agregar columnas adicionales si no existen
        nuevas_columnas = {
            "relevante": "BOOLEAN DEFAULT 0",
            "confianza_precio": "TEXT DEFAULT 'baja'",
            "muestra_precio": "INTEGER DEFAULT 0"
        }
        
        for nombre, definicion in nuevas_columnas.items():
            if nombre not in columnas_existentes:
                try:
                    cur.execute(f"ALTER TABLE anuncios ADD COLUMN {nombre} {definicion}")
                    print(f"✅ Columna '{nombre}' agregada")
                except sqlite3.OperationalError as e:
                    print(f"⚠️ Error al agregar columna '{nombre}': {e}")
        
        conn.commit()

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
    año_actual = datetime.now().year
    candidatos = [int(x) for x in matches if int(x) < 1990 or int(x) > año_actual + 1]
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





def extraer_anio(texto: str) -> Optional[int]:
    """
    Extrae el año del vehículo del texto con mayor precisión.
    Evita confundir precios con años.
    """
    if not texto or not isinstance(texto, str):
        return None

    texto_original = texto
    texto = texto.lower().strip()
    año_actual = datetime.now().year
    año_min = 1980
    año_max = min(año_actual + 2, 2027)

    # 🚫 PASO 1: FILTRAR CONTEXTOS CLARAMENTE NO VEHICULARES
    contextos_invalidos = [
        r"\b(se unió|miembro desde|ingresado en|empleado desde|activo en|registrado en|creado en|fecha de creación|nacido en|nació en)\s*:?\s*(19\d{2}|20\d{2})",
        r"\b(visto en|fecha de publicación|perfil creado en|último acceso|publicado en|actualizado)\s*:?\s*(19\d{2}|20\d{2})",
        r"\b(graduado en|casado en|fallecido en|murió en|titulado en)\s*:?\s*(19\d{2}|20\d{2})",
        r"\b(construido en|casa del|edificado en|vivienda del)\s*:?\s*(19\d{2}|20\d{2})",
        r"\b(código|id|tel|teléfono|celular|número)[\s\-_]*:?\s*\d*\s*(19\d{2}|20\d{2})",
        r"\b(calle|avenida|av|dirección|ubicado en).*?(19\d{2}|20\d{2})",
    ]
    
    for patron in contextos_invalidos:
        if re.search(patron, texto):
            return None

    # 🎯 PASO 2: PATRONES VEHICULARES EXPLÍCITOS CON MÁXIMA PRIORIDAD
    # Estos patrones tienen precedencia absoluta sobre todo lo demás
    patrones_explicitos_vehiculares = [
        # Años en títulos o al inicio
        r"^[^\n]*?\b(19\d{2}|20\d{2})\s+(?:hyundai|toyota|honda|nissan|ford|chevrolet|kia|suzuki|mitsubishi)\b",
        r"^[^\n]*?\b(?:hyundai|toyota|honda|nissan|ford|chevrolet|kia|suzuki|mitsubishi)\s+(19\d{2}|20\d{2})\b",
        
        # Patrones con palabras clave vehiculares específicas
        r"\b(?:año|modelo|del año|versión|m/)\s*[:\-/]?\s*(19\d{2}|20\d{2})\b",
        r"\b(?:año|modelo|del año|versión)\s*[:\-]?\s*['`´]?(\d{2})\b",  # "modelo 98", "M/98"
        r"\b(vehículo|carro|auto|moto|camión)\s+(?:del\s+)?(?:año\s+)?(19\d{2}|20\d{2}|['`´]?\d{2})\b",
        r"\b(19\d{2}|20\d{2}|['`´]?\d{2})\s+(?:año|modelo|carro|auto|vehículo)\b",
        
        # Marcas + años (específico para evitar confusión con precios)
        r"\b(hyundai|toyota|honda|nissan|ford|chevrolet|kia|suzuki|mitsubishi|accent|civic|corolla|sentra|yaris|cr-v|tucson|picanto|spark|march|swift|alto|rio|grand vitara)\s+(19\d{2}|20\d{2})\b",
        r"\b(19\d{2}|20\d{2})\s+(hyundai|toyota|honda|nissan|ford|chevrolet|kia|suzuki|mitsubishi|accent|civic|corolla|sentra|yaris|cr-v|tucson|picanto|spark|march|swift|alto|rio|grand vitara)\b",
        
        # Años abreviados con contexto vehicular fuerte
        r"\b['`´](\d{2})\s*(?:año|modelo|gs|lx|ex|sport|gls|turbo)?\b",  # "'98" o "`98"
    ]
    
    for patron in patrones_explicitos_vehiculares:
        matches = re.finditer(patron, texto)
        for match in matches:
            for group in match.groups():
                if group and group.isdigit():
                    año_candidato = int(group)
                    
                    # Convertir años de 2 dígitos
                    if año_candidato <= 99:
                        if año_candidato >= 80:  # 80-99 → 1980-1999
                            año_completo = 1900 + año_candidato
                        elif año_candidato <= 30:  # 00-30 → 2000-2030
                            año_completo = 2000 + año_candidato
                        else:
                            continue  # 31-79 son ambiguos
                    else:
                        año_completo = año_candidato
                    
                    if año_min <= año_completo <= año_max:
                        return año_completo

    # 🚫 PASO 3: IDENTIFICAR Y EXCLUIR PRECIOS ESPECÍFICAMENTE
    # Remover todos los números que claramente son precios
    texto_sin_precios = _remover_precios_del_texto_mejorado(texto)
    
    # 🔍 PASO 4: AÑOS ABREVIADOS CERCA DE MODELOS DE VEHÍCULOS
    modelos_detectados = [m for m in MODELOS_INTERES if m in texto_sin_precios]
    
    if modelos_detectados:
        for modelo in modelos_detectados:
            for match in re.finditer(re.escape(modelo), texto_sin_precios):
                # Contexto de 40 caracteres alrededor del modelo
                contexto_inicio = max(0, match.start() - 40)
                contexto_fin = min(len(texto_sin_precios), match.end() + 40)
                contexto_local = texto_sin_precios[contexto_inicio:contexto_fin]
                
                # Buscar años abreviados en ese contexto
                años_abreviados = re.findall(r"['`´]?(\d{2})\b", contexto_local)
                for año_str in años_abreviados:
                    año_corto = int(año_str)
                    
                    # Excluir números que claramente no son años
                    if año_corto in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]:  # Versiones de motor
                        continue
                    if 30 < año_corto < 80:  # Rango ambiguo
                        continue
                        
                    if año_corto >= 80:
                        año_completo = 1900 + año_corto
                    elif año_corto <= 30:
                        año_completo = 2000 + año_corto
                    else:
                        continue
                    
                    if año_min <= año_completo <= año_max:
                        return año_completo

    # 🧐 PASO 5: AÑOS COMPLETOS CON VALIDACIÓN MUY ESTRICTA
    candidatos_completos = []
    
    # Buscar años completos en el texto sin precios
    for match in re.finditer(r"\b(19\d{2}|20\d{2})\b", texto_sin_precios):
        año = int(match.group())
        
        if not (año_min <= año <= año_max):
            continue
        
        # Verificar que no sea un año muy reciente si está solo (probablemente precio)
        if año >= 2015:
            # Para años recientes, requerir contexto vehicular muy fuerte
            contexto_inicio = max(0, match.start() - 50)
            contexto_fin = min(len(texto_sin_precios), match.end() + 50)
            contexto = texto_sin_precios[contexto_inicio:contexto_fin]
            
            # Verificar si hay indicadores de que NO es un precio
            no_es_precio = any([
                re.search(r"\b(año|modelo|del|version)\s*[:\-]?\s*" + str(año), contexto),
                re.search(r"\b" + str(año) + r"\s+(año|modelo|gs|lx|ex|sport)", contexto),
                any(marca in contexto for marca in ["hyundai", "toyota", "honda", "nissan", "ford"]),
                any(modelo in contexto for modelo in modelos_detectados)
            ])
            
            if not no_es_precio:
                continue
                
        # Obtener contexto alrededor del año
        contexto_inicio = max(0, match.start() - 40)
        contexto_fin = min(len(texto_sin_precios), match.end() + 40)
        contexto = texto_sin_precios[contexto_inicio:contexto_fin]
        
        # Calcular score del contexto
        score = _score_contexto_vehicular_mejorado(contexto, modelos_detectados)
        
        # Bonus especial para años que aparecen en contextos específicos
        if re.search(r"\b" + str(año) + r"\s*\w*\s*[-–]\s*(deportivo|económico|jalando|motor|usado|buen)", contexto):
            score += 5
            
        # VERIFICACIÓN ADICIONAL: que no sea parte de un precio en el texto original
        pos_en_original = texto_original.lower().find(str(año))
        if pos_en_original != -1:
            contexto_original = texto_original[max(0, pos_en_original-25):pos_en_original+25]
            # Si aparece junto a símbolos de dinero, penalizar
            if re.search(r"[q$]\s*\d*[,.]*\s*\d*\s*" + str(año), contexto_original.lower()):
                score -= 15
            # Si aparece como parte de un número grande (precio), penalizar
            if re.search(r"\d+[,.]\d*" + str(año)[-3:], contexto_original):
                score -= 15
            
        candidatos_completos.append((año, score))
    
    # Retornar el año con mejor score si es suficientemente bueno
    if candidatos_completos:
        candidatos_completos.sort(key=lambda x: x[1], reverse=True)
        mejor_año, mejor_score = candidatos_completos[0]
        # Score más alto requerido para años recientes
        threshold = 5 if mejor_año >= 2010 else 3
        if mejor_score >= threshold:
            return mejor_año

    return None


def _remover_precios_del_texto_mejorado(texto: str) -> str:
    """
    Versión mejorada que remueve patrones de precios del texto más agresivamente.
    """
    # Patrones de precios más completos
    patrones_precio = [
        r"\bq\s*[\d,.\s]+\b",  # Q 14,000 o Q14000
        r"\$\s*[\d,.\s]+\b",   # $14,000
        r"\b\d{1,3}(?:[,.]\d{3})+\b",  # 14,000 o 14.000
        r"\bprecio\s*[:\-]?\s*[\d,.\s]+\b",  # precio: 14000
        r"\bvalor\s*[:\-]?\s*[\d,.\s]+\b",   # valor 14000
        r"\bcuesta\s*[\d,.\s]+\b",           # cuesta 14000
        r"\b[\d,.\s]+\s*quetzales?\b",       # 14000 quetzales
        r"\b[\d,.\s]+\s*mil\b",              # 14 mil
        r"\bnegociable\s*[\d,.\s]*\b",       # negociable 16000
        r"\bespecial[,\s]*no\s*negociable\b", # precio especial, no negociable
        
        # Patrones específicos para los casos problemáticos
        r"\b(precio|valor)\s*[:\-]?\s*q?\s*\d{1,2}[,.]\d{3}\b",  # precio Q16,000
        r"\bq\d{2}[,.]\d{3}\b",  # Q16,000 directo
        r"\b\d{2}[,.]\d{3}\s*(quetzales?|efectivo|negociable)\b",  # 16,000 quetzales
    ]
    
    texto_limpio = texto
    for patron in patrones_precio:
        texto_limpio = re.sub(patron, " ", texto_limpio, flags=re.IGNORECASE)
    
    # Limpiar espacios múltiples
    texto_limpio = re.sub(r'\s+', ' ', texto_limpio).strip()
    
    return texto_limpio






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
    años_ant = max(0, datetime.now().year - anio)
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
    
    # Verificar si existen las columnas adicionales
    cur.execute("PRAGMA table_info(anuncios)")
    columnas_existentes = {row[1] for row in cur.fetchall()}
    
    if all(col in columnas_existentes for col in ["relevante", "confianza_precio", "muestra_precio"]):
        # Insertar con columnas adicionales
        cur.execute("""
        INSERT OR REPLACE INTO anuncios 
        (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
        """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio))
    else:
        # Insertar solo con columnas básicas
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
        
        # Verificar si existe la columna confianza_precio
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

def obtener_anuncio_db(link: str) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT modelo, anio, precio, km, roi, score
            FROM anuncios
            WHERE link = ?
        """, (limpiar_link(link),))
        row = cur.fetchone()
        if row:
            return {
                "modelo": row[0],
                "anio": row[1],
                "precio": row[2],
                "km": row[3],
                "roi": row[4],
                "score": row[5]
            }
        return None

def anuncio_diferente(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    campos_clave = ["modelo", "anio", "precio", "km", "roi", "score"]
    return any(str(a.get(c)) != str(b.get(c)) for c in campos_clave)

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
        "url": limpiar_link(url),  # Cambié link por url para mantener consistencia
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
