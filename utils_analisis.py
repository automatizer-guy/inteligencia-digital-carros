import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager

def escapar_multilinea(texto: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 0
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
    "sin motor", "para partes", "no funciona", "accidentado", "partes disponibles, 
    "partes", "desarme", "solo piezas"
    
]

LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# Patrones precompilados para extraer año
_PATTERN_YEAR_FULL   = re.compile(r"\b(19\d{2}|20\d{2})\b")
_PATTERN_YEAR_SHORT  = re.compile(r"['`´]?(\d{2})\b")
_PATTERN_PRICE       = re.compile(
    r"\b(?:q|\$)?\s*[\d.,]+(?:\s*quetzales?)?\b",
    flags=re.IGNORECASE
)
_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro desde|publicado en|nacido en|creado en|registro|perfil creado)\b.*?(19\d{2}|20\d{2})",
    flags=re.IGNORECASE
)


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



def extraer_anio(texto: str, debug: bool = False) -> Optional[int]:
    """
    Extrae el año del vehículo del texto sin alterar la API existente.
    Mantiene compatibilidad con los módulos dependientes.
    """
    if not texto or not isinstance(texto, str):
        return None

    txt = texto.lower().strip()
    año_actual = datetime.now().year
    año_min, año_max = 1980, año_actual + 2

    if debug:
        print(f"[DEBUG] Texto: {txt[:80]}...")

    # Marcar contexto inválido (no aborta)
    invalid_ctx = bool(_PATTERN_INVALID_CTX.search(txt))
    if debug and invalid_ctx:
        print("[DEBUG] Contexto marcado inválido")

    # Buscar primeras apariciones de modelo
    apariciones: List[Tuple[int,str]] = []
    for modelo in MODELOS_INTERES:
        for m in re.finditer(re.escape(modelo.lower()), txt):
            apariciones.append((m.start(), modelo))
    apariciones.sort()

    def scan_ventana(center: int) -> List[Tuple[int,int,str]]:
        inicio, fin = max(0, center-40), min(len(txt), center+40)
        window = txt[inicio:fin]
        result: List[Tuple[int,int,str]] = []
        # Años completos
        for m in _PATTERN_YEAR_FULL.finditer(window):
            y = int(m.group())
            if año_min <= y <= año_max:
                score = 90 - abs(m.start()-40)
                result.append((y, score, 'full_near_model'))
        # Años abreviados
        for m in _PATTERN_YEAR_SHORT.finditer(window):
            y2 = int(m.group(1))
            y  = 1900+y2 if y2>=80 else 2000+y2
            if año_min <= y <= año_max:
                score = 100 - abs(m.start()-40)
                result.append((y, score, 'short_near_model'))
        return result

    candidatos: List[Tuple[int,int,str]] = []

    # 1) Ventana de la primera aparición de modelo
    if apariciones:
        pos, modelo = apariciones[0]
        if debug:
            print(f"[DEBUG] Usando modelo '{modelo}' en pos {pos}")
        candidatos.extend(scan_ventana(pos))

    # 2) Primeras dos líneas (títulos)
    for linea in txt.splitlines()[:2]:
        for m in _PATTERN_YEAR_FULL.finditer(linea):
            y = int(m.group())
            if año_min <= y <= año_max:
                candidatos.append((y, 85, 'titulo_full'))
        for m in _PATTERN_YEAR_SHORT.finditer(linea):
            y2 = int(m.group(1))
            y  = 1900+y2 if y2>=80 else 2000+y2
            if año_min <= y <= año_max:
                candidatos.append((y, 90, 'titulo_short'))

    # 3) Global sobre texto sin precios
    txt_no_price = _PATTERN_PRICE.sub(' ', txt)
    for m in _PATTERN_YEAR_FULL.finditer(txt_no_price):
        y = int(m.group())
        if año_min <= y <= año_max:
            score = 50 + (20 if any(marca.lower() in txt_no_price for marca in MODELOS_INTERES) else 0)
            if invalid_ctx: score -= 10
            candidatos.append((y, score, 'global_full'))
    for m in _PATTERN_YEAR_SHORT.finditer(txt_no_price):
        y2 = int(m.group(1))
        y  = 1900+y2 if y2>=80 else 2000+y2
        if año_min <= y <= año_max:
            score = 55 + (15 if any(marca.lower() in txt_no_price for marca in MODELOS_INTERES) else 0)
            if invalid_ctx: score -= 5
            candidatos.append((y, score, 'global_short'))

    # Selección final
    if not candidatos:
        if debug: print("[DEBUG] Sin candidatos")
        return None

    candidatos.sort(key=lambda x: x[1], reverse=True)
    mejor_y, mejor_score, motivo = candidatos[0]
    if debug:
        print(f"[DEBUG] Seleccionado {mejor_y} (score {mejor_score}, {motivo})")

    umbral = 30 if mejor_y >= 2010 else 25
    return mejor_y if mejor_score >= umbral else None


    # 🏆 PASO 3: PRIORIDAD MÁXIMA - AÑOS CERCA DEL MODELO DETECTADO
    if modelo_detectado and posicion_modelo >= 0:
        # Definir ventana alrededor del modelo (±40 caracteres)
        inicio_ventana = max(0, posicion_modelo - 40)
        fin_ventana = min(len(texto), posicion_modelo + len(modelo_detectado) + 40)
        ventana_modelo = texto[inicio_ventana:fin_ventana]
        
        if DEBUG:
            print(f"🎯 Ventana del modelo: '{ventana_modelo}'")
        
        # Buscar años en esta ventana con máxima prioridad
        candidatos_modelo = []
        
        # 1. Años abreviados muy cerca del modelo
        años_abrev_cerca = re.finditer(r"['`´]?(\d{2})\b", ventana_modelo)
        for match in años_abrev_cerca:
            año_str = match.group(1)
            año_corto = int(año_str)
            
            # Validaciones para años abreviados
            if año_corto in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]:  # Versiones de motor
                continue
            if 30 < año_corto < 80:  # Rango ambiguo, saltar
                continue
                
            if año_corto >= 80:
                año_completo = 1900 + año_corto
            elif año_corto <= 30:
                año_completo = 2000 + año_corto
            else:
                continue
            
            if año_min <= año_completo <= año_max:
                # Score muy alto por estar cerca del modelo
                distancia = abs(match.start() - 20)  # 20 es aprox. centro de ventana
                score = 100 - distancia  # Mientras más cerca, mejor score
                candidatos_modelo.append((año_completo, score, f"abreviado cerca del modelo"))
                if DEBUG:
                    print(f"✅ Año abreviado cerca del modelo: {año_completo} (score: {score})")
        
        # 2. Años completos muy cerca del modelo
        años_completos_cerca = re.finditer(r"\b(19\d{2}|20\d{2})\b", ventana_modelo)
        for match in años_completos_cerca:
            año = int(match.group(1))
            
            if not (año_min <= año <= año_max):
                continue
            
            # Para años muy recientes cerca del modelo, ser más permisivo
            distancia = abs(match.start() - 20)
            score = 90 - distancia
            
            # Verificar que no sea claramente un precio
            contexto_micro = ventana_modelo[max(0, match.start()-15):match.end()+15]
            if re.search(r"[q$]\s*\d*[,.]*\s*" + str(año), contexto_micro):
                score -= 50  # Penalizar mucho si parece precio
            
            candidatos_modelo.append((año, score, f"completo cerca del modelo"))
            if DEBUG:
                print(f"✅ Año completo cerca del modelo: {año} (score: {score})")
        
        # Si encontramos candidatos cerca del modelo, usar el mejor
        if candidatos_modelo:
            candidatos_modelo.sort(key=lambda x: x[1], reverse=True)
            mejor_año, mejor_score, razon = candidatos_modelo[0]
            if mejor_score > 50:  # Threshold para candidatos cerca del modelo
                if DEBUG:
                    print(f"🏆 AÑO SELECCIONADO (cerca del modelo): {mejor_año} - {razon}")
                return mejor_año

    # 🔍 PASO 4: TÍTULOS Y PRIMERAS LÍNEAS (alta prioridad)
    lineas = texto.split('\n')
    primeras_lineas = lineas[:2] if len(lineas) >= 2 else [texto]
    
    for i, linea in enumerate(primeras_lineas):
        if DEBUG:
            print(f"📝 Analizando línea {i+1}: '{linea}'")
        
        # Patrones vehiculares explícitos en títulos
        patrones_titulo = [
            r"\b(19\d{2}|20\d{2})\s+(?:hyundai|toyota|honda|nissan|ford|chevrolet|kia|suzuki|mitsubishi)\b",
            r"\b(?:hyundai|toyota|honda|nissan|ford|chevrolet|kia|suzuki|mitsubishi)\s+(19\d{2}|20\d{2})\b",
            r"\b(accent|civic|corolla|sentra|yaris|cr-v|tucson|picanto|spark|march|swift|alto|rio)\s+(19\d{2}|20\d{2}|'?\d{2})\b",
            r"\b(19\d{2}|20\d{2}|'?\d{2})\s+(accent|civic|corolla|sentra|yaris|cr-v|tucson|picanto|spark|march|swift|alto|rio)\b",
        ]
        
        for patron in patrones_titulo:
            matches = re.finditer(patron, linea)
            for match in matches:
                for group in match.groups():
                    if group and group.replace("'", "").replace("`", "").replace("´", "").isdigit():
                        año_str = group.replace("'", "").replace("`", "").replace("´", "")
                        año_candidato = int(año_str)
                        
                        # Convertir años de 2 dígitos
                        if año_candidato <= 99:
                            if año_candidato >= 80:
                                año_completo = 1900 + año_candidato
                            elif año_candidato <= 30:
                                año_completo = 2000 + año_candidato
                            else:
                                continue
                        else:
                            año_completo = año_candidato
                        
                        if año_min <= año_completo <= año_max:
                            if DEBUG:
                                print(f"🏆 AÑO SELECCIONADO (título): {año_completo}")
                            return año_completo

    # 🧹 PASO 5: REMOVER PRECIOS DEL TEXTO PARA ANÁLISIS GENERAL
    texto_sin_precios = _remover_precios_del_texto_mejorado(texto)
    
    if DEBUG:
        print(f"🧹 Texto sin precios: {texto_sin_precios[:100]}...")

    # 📊 PASO 6: ANÁLISIS GENERAL CON VALIDACIÓN ESTRICTA
    candidatos_generales = []
    
    # Buscar años completos en texto sin precios
    for match in re.finditer(r"\b(19\d{2}|20\d{2})\b", texto_sin_precios):
        año = int(match.group())
        
        if not (año_min <= año <= año_max):
            continue
        
        # Obtener contexto alrededor del año
        contexto_inicio = max(0, match.start() - 50)
        contexto_fin = min(len(texto_sin_precios), match.end() + 50)
        contexto = texto_sin_precios[contexto_inicio:contexto_fin]
        
        # Calcular score del contexto
        score = _score_contexto_vehicular_mejorado(contexto, [modelo_detectado] if modelo_detectado else [])
        
        # VALIDACIÓN MUY ESTRICTA PARA AÑOS RECIENTES (2010+)
        if año >= 2010:
            score -= 20  # Penalización base para años recientes
            
            # Debe estar muy cerca de indicadores vehiculares
            palabras_vehiculares_fuertes = ['modelo', 'año', 'version', 'del', 'motor', 'carro', 'auto']
            if modelo_detectado:
                palabras_vehiculares_fuertes.append(modelo_detectado)
            
            tiene_contexto_fuerte = any(palabra in contexto for palabra in palabras_vehiculares_fuertes)
            if not tiene_contexto_fuerte:
                score -= 30
                if DEBUG:
                    print(f"❌ Año {año} descartado por falta de contexto vehicular fuerte")
                continue
            
            # Verificar que no sea parte de un precio en el texto original
            pos_en_original = texto_original.lower().find(str(año))
            if pos_en_original != -1:
                contexto_original = texto_original[max(0, pos_en_original-30):pos_en_original+30]
                if re.search(r"[q$]\s*\d*[,.]*\s*\d*\s*" + str(año), contexto_original.lower()):
                    score -= 50
                    if DEBUG:
                        print(f"❌ Año {año} descartado por aparecer como precio")
                    continue
        
        # Bonus para contextos específicos
        if re.search(r"\b" + str(año) + r"\s*(mecánico|automático|motor|bien|excelente)", contexto):
            score += 10
            
        candidatos_generales.append((año, score))
        if DEBUG:
            print(f"📊 Candidato general: {año} (score: {score})")
    
    # Buscar años abreviados en contexto general
    años_abrev_general = re.finditer(r"['`´](\d{2})\b", texto_sin_precios)
    for match in años_abrev_general:
        año_str = match.group(1)
        año_corto = int(año_str)
        
        if año_corto in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]:
            continue
        if 30 < año_corto < 80:
            continue
            
        if año_corto >= 80:
            año_completo = 1900 + año_corto
        elif año_corto <= 30:
            año_completo = 2000 + año_corto
        else:
            continue
        
        if año_min <= año_completo <= año_max:
            contexto_inicio = max(0, match.start() - 30)
            contexto_fin = min(len(texto_sin_precios), match.end() + 30)
            contexto = texto_sin_precios[contexto_inicio:contexto_fin]
            
            score = _score_contexto_vehicular_mejorado(contexto, [modelo_detectado] if modelo_detectado else [])
            score += 5  # Bonus por ser año abreviado (más probable en autos antiguos)
            
            candidatos_generales.append((año_completo, score))
            if DEBUG:
                print(f"📊 Candidato abreviado general: {año_completo} (score: {score})")
    
    # Seleccionar el mejor candidato general
    if candidatos_generales:
        candidatos_generales.sort(key=lambda x: x[1], reverse=True)
        mejor_año, mejor_score = candidatos_generales[0]
        
        # Threshold más alto para candidatos generales
        threshold_requerido = 8 if mejor_año >= 2010 else 5
        
        if mejor_score >= threshold_requerido:
            if DEBUG:
                print(f"🏆 AÑO SELECCIONADO (general): {mejor_año} (score: {mejor_score})")
            return mejor_año
        elif DEBUG:
            print(f"❌ Mejor candidato {mejor_año} rechazado por score insuficiente: {mejor_score} < {threshold_requerido}")

    if DEBUG:
        print("❌ No se pudo extraer un año válido")
    return None


def _remover_precios_del_texto_mejorado(texto: str) -> str:
    """
    Versión mejorada que remueve patrones de precios del texto más agresivamente.
    """
    # Patrones de precios más completos y específicos
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
        
        # Patrones específicos para casos problemáticos
        r"\b(precio|valor)\s*[:\-]?\s*q?\s*\d{1,2}[,.]\d{3}\b",  # precio Q16,000
        r"\bq\d{2}[,.]\d{3}\b",  # Q16,000 directo
        r"\b\d{2}[,.]\d{3}\s*(quetzales?|efectivo|negociable)\b",  # 16,000 quetzales
        
        # Patrones para precios en formato completo
        r"\b\d{4,6}\s*(quetzales?|efectivo|negociable|final)\b",  # 15000 quetzales
        r"\b(Q|q)\s*\d{4,6}\b",  # Q15000
    ]
    
    texto_limpio = texto
    for patron in patrones_precio:
        texto_limpio = re.sub(patron, " ", texto_limpio, flags=re.IGNORECASE)
    
    # Limpiar espacios múltiples
    texto_limpio = re.sub(r'\s+', ' ', texto_limpio).strip()
    
    return texto_limpio


def _score_contexto_vehicular_mejorado(texto: str, modelos_detectados: List[str] = None) -> int:
    """
    Calcula un score mejorado de qué tan probable es que el contexto sea vehicular.
    """
    if modelos_detectados is None:
        modelos_detectados = []
    
    puntuacion = 0
    
    # BONUS MUY FUERTE: Si hay modelos de vehículos detectados cerca
    if modelos_detectados:
        for modelo in modelos_detectados:
            if modelo and modelo in texto:
                puntuacion += 10  # Bonus muy alto
    
    # PALABRAS VEHICULARES MUY FUERTES (+5 cada una)
    vehiculares_muy_fuertes = [
        r"\b(modelo|año|del año|versión|m/)\b",
        r"\b(carro|auto|vehículo|camioneta|pickup)\b",
        r"\b(motor|transmisión|mecánico|automático)\b",
    ]
    
    # PALABRAS VEHICULARES FUERTES (+3 cada una)
    vehiculares_fuertes = [
        r"\b(toyota|honda|nissan|ford|chevrolet|volkswagen|hyundai|kia|mazda|mitsubishi|suzuki)\b",
        r"\b(sedan|hatchback|suv|coupe)\b",
        r"\b(kilometraje|km|millas|gasolina|diésel)\b"
    ]
    
    # PALABRAS VEHICULARES MODERADAS (+1 cada una)
    vehiculares_moderadas = [
        r"\b(usado|seminuevo|equipado|papeles|documentos|traspaso)\b",
        r"\b(llantas|frenos|batería|aceite|aire acondicionado)\b",
        r"\b(bien cuidado|excelente estado|poco uso)\b"
    ]
    
    # PALABRAS NEGATIVAS (-5 cada una)
    penalizaciones_fuertes = [
        r"\b(casa|departamento|oficina|vivienda|terreno|local)\b",
        r"\b(perfil|usuario|miembro|facebook|página)\b",
        r"\b(teléfono|celular|contacto|whatsapp|email)\b"
    ]
    
    # PALABRAS NEGATIVAS MODERADAS (-2 cada una)
    penalizaciones_moderadas = [
        r"\b(nacido|empleado|graduado|familia|matrimonio)\b",
        r"\b(publicado|creado|actualizado|visto)\b"
    ]
    
    for patron in vehiculares_muy_fuertes:
        puntuacion += 5 * len(re.findall(patron, texto, re.IGNORECASE))
    
    for patron in vehiculares_fuertes:
        puntuacion += 3 * len(re.findall(patron, texto, re.IGNORECASE))
    
    for patron in vehiculares_moderadas:
        puntuacion += 1 * len(re.findall(patron, texto, re.IGNORECASE))
    
    for patron in penalizaciones_fuertes:
        puntuacion -= 5 * len(re.findall(patron, texto, re.IGNORECASE))
        
    for patron in penalizaciones_moderadas:
        puntuacion -= 2 * len(re.findall(patron, texto, re.IGNORECASE))
    
    return max(0, puntuacion)




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
