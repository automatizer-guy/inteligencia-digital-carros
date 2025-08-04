import os
import re
import sqlite3
import time
import unicodedata
import statistics
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Set
from contextlib import contextmanager
from correcciones import obtener_correccion

def escapar_multilinea(texto: str) -> str:
    return re.sub(r'([_*\[\]()~`>#+=|{}.!\\-])', r'\\\1', texto)

DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DEBUG = os.getenv("DEBUG", "False").lower() in ("1", "true", "yes")
SCORE_MIN_DB = 0
SCORE_MIN_TELEGRAM = 20  # Balanceado - ni muy estricto ni muy permisivo
ROI_MINIMO = 8.0  # Balanceado para oportunidades reales
TOLERANCIA_PRECIO_REF = 1
DEPRECIACION_ANUAL = 0.08
MUESTRA_MINIMA_CONFIABLE = 5
MUESTRA_MINIMA_MEDIA = 2
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 1980
MAX_YEAR = CURRENT_YEAR + 1

# ============================================================================
# CONFIGURACIÓN BALANCEADA DEL SCORING ENGINE
# ============================================================================
# Pesos para diferentes fuentes de año (mantener alta diferenciación)
WEIGHT_MODEL      = 100   # Año cerca del modelo - máxima confianza
WEIGHT_TITLE      = 85    # Año con palabras clave (modelo, versión, etc.)
WEIGHT_WINDOW     = 70    # Año completo (4 dígitos) en contexto vehicular
WEIGHT_GENERAL    = 50    # Año encontrado en texto general

# Penalizaciones (moderadas para evitar descartes excesivos)
PENALTY_INVALID   = -40   # Contextos engañosos
PENALTY_NO_KM     = -10   # Sin kilometraje especificado
PENALTY_NEGATIVAS = -25   # Palabras negativas críticas

# Bonificaciones (generosas para promover detección)
BONUS_VEHICULO    = 15    # Palabras vehiculares
BONUS_PRECIO_HIGH = 12    # Precio coherente con año
BONUS_CONTEXTO_FUERTE = 18  # Contexto muy vehicular
BONUS_ROI_EXCELENTE = 25    # ROI >= ROI_MINIMO
BONUS_ROI_BUENO = 15        # ROI >= 5

# ============================================================================
# DATOS DE REFERENCIA
# ============================================================================
PRECIOS_POR_DEFECTO = {
    "yaris": 45000, "civic": 65000, "corolla": 50000, "sentra": 42000,
    "rav4": 130000, "cr-v": 95000, "tucson": 70000, "kia picanto": 35000,
    "chevrolet spark": 30000, "nissan march": 37000, "suzuki alto": 26000,
    "suzuki swift": 40000, "hyundai accent": 41000, "mitsubishi mirage": 33000,
    "suzuki grand vitara": 52000, "hyundai i10": 34000, "kia rio": 40000,
    "toyota": 48000, "honda": 50000
}
MODELOS_INTERES = list(PRECIOS_POR_DEFECTO.keys())

# Contextos negativos críticos (descarte inmediato)
CONTEXTOS_CRITICOS_NEGATIVOS = [
    "solo repuestos", "para repuestos", "desarme completo", "motor fundido", 
    "no arranca", "no enciende", "sin motor", "para partes solamente", 
    "no funciona", "accidentado grave", "partes disponibles", "chocado total"
]

# Contextos negativos leves (penalización menor)
CONTEXTOS_NEGATIVOS_LEVES = [
    "repuesto", "repuestos", "algunas piezas", "partes menores", "detalles"
]

# Lugares extranjeros
LUGARES_EXTRANJEROS = [
    "mexico", "ciudad de méxico", "monterrey", "usa", "estados unidos",
    "honduras", "el salvador", "panamá", "costa rica", "colombia", "ecuador"
]

# ============================================================================
# PATRONES REGEX PRECOMPILADOS (SIMPLES Y ROBUSTOS)
# ============================================================================
# Patrones para años - ORDEN IMPORTANTE: de más específico a más general
_PATTERN_YEAR_FULL = re.compile(r"\b(19\d{2}|20\d{2})\b")  # 1980-2099
_PATTERN_YEAR_SHORT = re.compile(r"['`´]?(\d{2})\b")       # '99, 99, etc.
_PATTERN_INVALID_CTX = re.compile(
    r"\b(?:miembro desde|publicado en|nacido en|creado en|registro|Se unió a Facebook en|perfil creado|calcomania del:)\b.*?(19\d{2}|20\d{2})",
    flags=re.IGNORECASE
)

# Sinónimos extensos para modelos (manteniendo la robustez del código original)
sinonimos = {
    "yaris": [
        "yaris", "toyota yaris", "new yaris", "yaris sedan", "yaris hatchback", "yaris hb",
        "vitz", "toyota vitz", "platz", "toyota platz", "echo", "toyota echo", 
        "belta", "toyota belta", "vios", "toyota vios",
        "yaris core", "yaris s", "yaris xls", "yaris xle", "yaris le", "yaris l",
        "yaris spirit", "yaris sport", "yaris cross", "yaris ia", "yaris r",
        "yariz", "toyoya yaris", "toyota yariz", "yaris toyota"
    ],
    
    "civic": [
        "civic", "honda civic", "civic sedan", "civic hatchback", "civic coupe",
        "civic type r", "civic si", "civic sir", "civic ex", "civic lx", "civic dx",
        "civic vti", "civic esi", "civic ls", "civic hybrid", "civic touring",
        "civic eg", "civic ek", "civic em", "civic es", "civic ep", "civic eu",
        "civic fn", "civic fa", "civic fd", "civic fb", "civic fc", "civic fk",
        "civc", "civic honda", "honda civik", "civick"
    ],
    
    "corolla": [
        "corolla", "toyota corolla", "corolla sedan", "corolla hatchback",
        "corolla cross", "corolla altis", "corolla axio", "corolla fielder",
        "corolla le", "corolla s", "corolla l", "corolla xle", "corolla se",
        "toyota corola", "corola", "corollo", "corolla toyota"
    ],
    
    "sentra": [
        "sentra", "nissan sentra", "sentra sedan", "sentra clasico", "sentra clásico",
        "sentra b13", "nissan b13", "sentra b14", "sentra b15", "sentra b16",
        "sentra gxe", "sentra se", "sentra xe", "sentra e", "sentra gx",
        "sunny", "nissan sunny", "tsuru", "nissan tsuru", "almera", "nissan almera",
        "sentran", "nissan sentran"
    ],
    
    "rav4": [
        "rav4", "rav-4", "toyota rav4", "toyota rav-4", "rav 4", "toyota rav 4",
        "rav4 le", "rav4 xle", "rav4 limited", "rav4 sport", "rav4 adventure",
        "rab4", "toyota rab4", "raw4"
    ],
    
    "cr-v": [
        "cr-v", "crv", "honda cr-v", "honda crv", "cr v", "honda cr v",
        "cr-v lx", "cr-v ex", "cr-v ex-l", "cr-v touring", "crv lx", "crv ex",
        "cr b", "honda cr b"
    ],
    
    "tucson": [
        "tucson", "hyundai tucson", "tuczon", "tucsón", "tucson suv",
        "tucson gls", "tucson se", "tucson limited", "ix35",
        "hyundai tuczon", "tucson hyundai"
    ],
    
    "kia picanto": [
        "picanto", "kia picanto", "picanto hatchbook", "morning", "kia morning",
        "pikanto", "kia pikanto"
    ],
    
    "chevrolet spark": [
        "spark", "chevrolet spark", "chevy spark", "matiz", "chevrolet matiz",
        "beat", "chevrolet beat", "sp4rk"
    ],
    
    "nissan march": [
        "march", "nissan march", "micra", "nissan micra", "note", "nissan note",
        "m4rch", "nissan m4rch"
    ],
    
    "suzuki alto": [
        "alto", "suzuki alto", "celerio", "suzuki celerio", "alt0", "suzuki alt0"
    ],
    
    "suzuki swift": [
        "swift", "suzuki swift", "swift hatchbook", "swift gl", "swift gls",
        "swft", "suzuki swft"  
    ],
    
    "hyundai accent": [
        "accent", "hyundai accent", "verna", "hyundai verna", "excel", "hyundai excel",
        "acent", "hyundai acent"
    ],
    
    "mitsubishi mirage": [
        "mirage", "mitsubishi mirage", "space star", "mitsubishi space star",
        "attrage", "mitsubishi attrage", "miraje"
    ],
    
    "suzuki grand vitara": [
        "grand vitara", "suzuki grand vitara", "gran vitara", "vitara", "suzuki vitara",
        "escudo", "suzuki escudo", "tracker", "chevrolet tracker"
    ],
    
    "hyundai i10": [
        "i10", "hyundai i10", "i-10", "hyundai i-10", "atos", "hyundai atos",
        "santro", "hyundai santro", "grand i10", "hyundai grand i10"
    ],
    
    "kia rio": [
        "rio", "kia rio", "pride", "kia pride", "rio5", "kia rio5",
        "kia ryo", "ryo"
    ],
    
    "toyota": ["toyota", "toyoya", "toyata"],
    "honda": ["honda", "hondas"]
}

# ============================================================================
# CACHE EN MEMORIA PARA OPTIMIZACIÓN DE RENDIMIENTO
# ============================================================================
class CacheAnuncios:
    """Cache en memoria para evitar consultas repetitivas a la base de datos"""
    
    def __init__(self):
        self._cache_existentes: Dict[str, Set[str]] = {}
        self._ultimo_refresh: Dict[str, float] = {}
        self._ttl = 300  # 5 minutos
    
    def get_existentes(self, modelo: str) -> Set[str]:
        """Obtiene links existentes para un modelo, usando cache si es válido"""
        now = time.time()
        
        # Verificar si el cache es válido
        if (modelo in self._cache_existentes and 
            modelo in self._ultimo_refresh and 
            now - self._ultimo_refresh[modelo] < self._ttl):
            return self._cache_existentes[modelo]
        
        # Actualizar cache
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT link FROM anuncios WHERE modelo = ?", (modelo,))
            links = {row[0] for row in cur.fetchall()}
        
        self._cache_existentes[modelo] = links
        self._ultimo_refresh[modelo] = now
        
        if DEBUG:
            print(f"🔄 Cache actualizado para {modelo}: {len(links)} anuncios existentes")
        
        return links
    
    def invalidar(self, modelo: str = None):
        """Invalida el cache para un modelo específico o todo el cache"""
        if modelo:
            self._cache_existentes.pop(modelo, None)
            self._ultimo_refresh.pop(modelo, None)
        else:
            self._cache_existentes.clear()
            self._ultimo_refresh.clear()

# Instancia global del cache
_cache_anuncios = CacheAnuncios()

# ============================================================================
# UTILIDADES Y DECORADORES
# ============================================================================
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

# ============================================================================
# FUNCIONES DE PREPROCESAMIENTO Y LIMPIEZA
# ============================================================================
def normalizar_formatos_ano(texto: str) -> str:
    """Convierte 2,009 o 2.009 → 2009"""
    return re.sub(r'\b(\d)[,\.](\d{3})\b', r'\1\2', texto)

def limpiar_emojis_numericos(texto: str) -> str:
    """Convierte emojis numéricos a dígitos normales"""
    mapa_emojis = {
        '0️⃣': '0', '1️⃣': '1', '2️⃣': '2', '3️⃣': '3', '4️⃣': '4',
        '5️⃣': '5', '6️⃣': '6', '7️⃣': '7', '8️⃣': '8', '9️⃣': '9',
        '⓪': '0', '①': '1', '②': '2', '③': '3', '④': '4',
        '⑤': '5', '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9'
    }
    for emoji, digito in mapa_emojis.items():
        texto = texto.replace(emoji, digito)
    return texto

def limpiar_link(link: Optional[str]) -> str:
    """Limpia y normaliza links para almacenamiento"""
    if not link:
        return ""
    return ''.join(c for c in link.strip() if c.isascii() and c.isprintable())

def limpiar_precio(texto: str) -> int:
    """
    Extrae precio del texto - VERSIÓN CORREGIDA
    Excluye años del rango de precios válidos
    """
    s = re.sub(r"[Qq\$\.,]", "", texto.lower())
    matches = re.findall(r"\b\d{3,7}\b", s)
    # CORRECCIÓN: Excluir años válidos de los candidatos a precio
    candidatos = [int(x) for x in matches if not (MIN_YEAR <= int(x) <= MAX_YEAR)]
    return candidatos[0] if candidatos else 0

# ============================================================================
# FUNCIÓN PRINCIPAL: EXTRACCIÓN DE AÑO HÍBRIDA Y ROBUSTA
# ============================================================================
def extraer_anio(texto: str, modelo: str = None, precio: int = None, debug: bool = False) -> Optional[int]:
    """
    FUNCIÓN HÍBRIDA Y ROBUSTA - Lógica en cascada con return inmediato
    
    Estrategia:
    1. Corrección manual (máxima prioridad)
    2. Año cerca del modelo del vehículo (alta confianza)
    3. Año con palabras clave vehiculares (confianza media)
    4. Año completo en contexto limpio (confianza básica)
    5. Año corto como último recurso (baja confianza)
    """
    if debug:
        print(f"\n🔍 Extrayendo año de: {texto[:80]}...")
    
    # Preprocesamiento
    texto = limpiar_emojis_numericos(texto)
    texto = normalizar_formatos_ano(texto)
    texto_original = texto
    texto_lower = texto.lower()
    
    # ========================================================================
    # ESTRATEGIA 1: CORRECCIÓN MANUAL (MÁXIMA PRIORIDAD)
    # ========================================================================
    correccion_manual = obtener_correccion(texto_original)
    if correccion_manual:
        if debug:
            print(f"✅ Corrección manual: {correccion_manual}")
        return correccion_manual
    
    # ========================================================================
    # ESTRATEGIA 2: AÑO CERCA DEL MODELO (ALTA CONFIANZA)
    # ========================================================================
    if modelo:
        variantes_modelo = sinonimos.get(modelo.lower(), [modelo.lower()])
        
        for variante in variantes_modelo:
            # Buscar la variante en el texto
            idx = texto_lower.find(variante)
            if idx != -1:
                # Extraer ventana de contexto alrededor del modelo
                inicio = max(0, idx - 20)
                fin = min(len(texto), idx + len(variante) + 20)
                ventana = texto[inicio:fin]
                
                # Buscar años en la ventana (primero 4 dígitos, luego 2)
                for match in _PATTERN_YEAR_FULL.finditer(ventana):
                    año_raw = match.group(1)
                    año = int(año_raw)
                    if MIN_YEAR <= año <= MAX_YEAR:
                        if debug:
                            print(f"✅ Año cerca del modelo '{variante}': {año}")
                        return año
                
                # Si no hay año completo, buscar año corto
                for match in _PATTERN_YEAR_SHORT.finditer(ventana):
                    año_raw = match.group(1)
                    if año_raw.isdigit() and len(año_raw) == 2:
                        año = normalizar_año_corto(int(año_raw))
                        if año and MIN_YEAR <= año <= MAX_YEAR:
                            if debug:
                                print(f"✅ Año corto cerca del modelo '{variante}': {año}")
                            return año
    
    # ========================================================================
    # ESTRATEGIA 3: AÑO CON PALABRAS CLAVE VEHICULARES (CONFIANZA MEDIA)
    # ========================================================================
    palabras_clave = [
        r"(modelo|m/|versión|año|del año|m\.|modelo:|año:)",
        r"(vendo|se vende|en venta)",
        r"(toyota|honda|nissan|ford|chevrolet|hyundai|kia|mazda|mitsubishi|suzuki)"
    ]
    
    for palabra_clave in palabras_clave:
        pattern = re.compile(f"{palabra_clave}[^\\d]{{0,10}}(19\\d{{2}}|20\\d{{2}})", re.IGNORECASE)
        match = pattern.search(texto)
        if match:
            año = int(match.group(2))
            if MIN_YEAR <= año <= MAX_YEAR:
                if debug:
                    print(f"✅ Año con palabra clave: {año}")
                return año
    
    # ========================================================================
    # ESTRATEGIA 4: AÑO COMPLETO EN CONTEXTO LIMPIO (CONFIANZA BÁSICA)
    # ========================================================================
    # Remover contextos inválidos
    texto_limpio = _PATTERN_INVALID_CTX.sub("", texto)
    
    for match in _PATTERN_YEAR_FULL.finditer(texto_limpio):
        año = int(match.group(1))
        if MIN_YEAR <= año <= MAX_YEAR:
            # Verificar que no esté en contexto sospechoso
            contexto = texto_limpio[max(0, match.start()-15):match.end()+15].lower()
            
            # Descartar si está en contexto claramente no vehicular
            if any(palabra in contexto for palabra in ["nacido", "miembro", "perfil", "facebook", "teléfono"]):
                continue
            
            if debug:
                print(f"✅ Año completo en contexto limpio: {año}")
            return año
    
    # ========================================================================
    # ESTRATEGIA 5: AÑO CORTO COMO ÚLTIMO RECURSO (BAJA CONFIANZA)
    # ========================================================================
    # Solo buscar años cortos en contexto vehicular fuerte
    if any(palabra in texto_lower for palabra in ["modelo", "año", "vendo", "toyota", "honda", "nissan"]):
        for match in _PATTERN_YEAR_SHORT.finditer(texto):
            año_raw = match.group(1)
            if año_raw.isdigit() and len(año_raw) == 2:
                año = normalizar_año_corto(int(año_raw))
                if año and MIN_YEAR <= año <= MAX_YEAR:
                    # Verificar contexto vehicular alrededor
                    contexto = texto[max(0, match.start()-20):match.end()+20].lower()
                    if any(palabra in contexto for palabra in ["modelo", "año", "vendo", "auto", "carro"]):
                        if debug:
                            print(f"✅ Año corto en contexto vehicular: {año}")
                        return año
    
    if debug:
        print("❌ No se pudo extraer año válido")
    return None

def normalizar_año_corto(año_corto: int) -> Optional[int]:
    """Normaliza años de 2 dígitos a 4 dígitos"""
    if año_corto < 0 or año_corto > 99:
        return None
    
    # Lógica: 80-99 → 1980-1999, 00-30 → 2000-2030
    if 80 <= año_corto <= 99:
        return 1900 + año_corto
    elif 0 <= año_corto <= 30:
        return 2000 + año_corto
    else:
        # Para años entre 31-79, asumimos que son más probablemente 2000+
        return 2000 + año_corto

# ============================================================================
# FUNCIONES DE VALIDACIÓN Y EVALUACIÓN
# ============================================================================
def evaluar_contexto_negativo(texto: str) -> Tuple[bool, int]:
    """
    Evalúa contexto negativo con dos niveles: crítico (descarte) y leve (penalización)
    """
    texto_lower = texto.lower()
    
    # Verificar contextos críticos (descarte automático)
    for contexto_critico in CONTEXTOS_CRITICOS_NEGATIVOS:
        if contexto_critico in texto_lower:
            return True, -100
    
    # Verificar contextos leves (penalización menor)
    penalizacion = 0
    for contexto_leve in CONTEXTOS_NEGATIVOS_LEVES:
        if contexto_leve in texto_lower:
            penalizacion -= 3
    
    return False, penalizacion

def validar_precio_coherente(precio: int, modelo: str, anio: int) -> Tuple[bool, str]:
    """
    Validación más permisiva de precios
    """
    if precio < 2000:
        return False, "precio_muy_bajo"
    if precio > 600000:
        return False, "precio_muy_alto"
    
    # Validación por edad del vehículo
    antiguedad = CURRENT_YEAR - anio
    if antiguedad < 0:
        return False, "anio_futuro"
    
    # Precios mínimos por antigüedad (más permisivos)
    if antiguedad <= 3 and precio < 8000:
        return False, "muy_nuevo_muy_barato"
    if antiguedad >= 30 and precio > 80000:
        return False, "muy_viejo_muy_caro"
    
    # Validación por modelo con márgenes amplios
    ref_info = get_precio_referencia(modelo, anio)
    precio_ref = ref_info.get("precio", PRECIOS_POR_DEFECTO.get(modelo, 50000))
    muestra = ref_info.get("muestra", 0)

    if muestra >= MUESTRA_MINIMA_CONFIABLE:
        margen_bajo = 0.15 * precio_ref
        margen_alto = 3.5 * precio_ref
    else:
        margen_bajo = 0.10 * precio_ref
        margen_alto = 4.0 * precio_ref

    if precio < margen_bajo:
        return False, "precio_sospechosamente_bajo"
    if precio > margen_alto:
        return False, "precio_muy_alto_para_modelo"
    
    return True, "valido"

def coincide_modelo(texto: str, modelo: str) -> bool:
    """Verifica si el texto contiene el modelo de vehículo"""
    texto_l = unicodedata.normalize("NFKD", texto.lower())
    modelo_l = modelo.lower()
    
    variantes = sinonimos.get(modelo_l, []) + [modelo_l]
    texto_limpio = unicodedata.normalize("NFKD", texto_l).encode("ascii", "ignore").decode("ascii")
    return any(v in texto_limpio for v in variantes)

def es_extranjero(texto: str) -> bool:
    """Verifica si el anuncio parece ser de otro país"""
    return any(lugar in texto.lower() for lugar in LUGARES_EXTRANJEROS)

# ============================================================================
# SCORING ENGINE OPTIMIZADO E INTELIGENTE
# ============================================================================
class ScoringEngine:
    """
    Motor de scoring híbrido que combina la inteligencia del codigo.py
    con la robustez del Codigo2.py
    """
    
    def __init__(self):
        self.threshold_descarte = -30      # Más permisivo
        self.threshold_relevante = SCORE_MIN_TELEGRAM
    
    def evaluar_anuncio_rapido(self, anuncio_data: dict, existentes: Set[str]) -> dict:
        """
        EVALUACIÓN RÁPIDA CON SHORT-CIRCUITS para máximo rendimiento
        
        Orden de validaciones para máxima eficiencia:
        1. ¿Es duplicado?
        2. ¿Faltan datos críticos?
        3. ¿Contexto negativo crítico?
        4. ¿Precio inválido?
        5. Solo entonces calcular score completo
        """
        # SHORT-CIRCUIT 1: Verificar duplicado (más rápido)
        link = anuncio_data.get("url", "")
        if link and limpiar_link(link) in existentes:
            return {
                "score": 0,
                "descartado": True,
                "razon_descarte": "duplicado",
                "relevante": False,
                "es_duplicado": True
            }
        
        # SHORT-CIRCUIT 2: Datos críticos faltantes
        titulo = anuncio_data.get("titulo", "")
        precio = anuncio_data.get("precio", 0)
        
        if not titulo or not precio:
            return {
                "score": -50,
                "descartado": True,
                "razon_descarte": "datos_faltantes",
                "relevante": False
            }
        
        # SHORT-CIRCUIT 3: Contexto negativo crítico
        es_critico, _ = evaluar_contexto_negativo(titulo)
        if es_critico:
            return {
                "score": -100,
                "descartado": True,
                "razon_descarte": "contexto_critico_negativo",
                "relevante": False
            }
        
        # SHORT-CIRCUIT 4: Extracción de año (puede fallar rápido)
        modelo = anuncio_data.get("modelo", "")
        anio = extraer_anio(titulo, modelo, precio)
        
        if not anio:
            return {
                "score": -40,
                "descartado": True,
                "razon_descarte": "sin_anio_valido",
                "relevante": False
            }
        
        # Completar datos para evaluación completa
        anuncio_completo = {**anuncio_data, "anio": anio}
        
        # SHORT-CIRCUIT 5: Validación de precio
        precio_valido, razon_precio = validar_precio_coherente(precio, modelo, anio)
        if not precio_valido:
            return {
                "score": -35,
                "descartado": True,
                "razon_descarte": f"precio_invalido_{razon_precio}",
                "relevante": False
            }
        
        # Si pasa todos los short-circuits, hacer evaluación completa
        return self.evaluar_anuncio_completo(anuncio_completo)
    
    def evaluar_anuncio_completo(self, anuncio_data: dict) -> dict:
        """
        Evaluación completa usando el sistema inteligente de scoring
        """
        score = 0
        razones = []
        
        texto = anuncio_data.get("titulo", "")
        modelo = anuncio_data.get("modelo", "")
        anio = anuncio_data.get("anio", CURRENT_YEAR)
        precio = anuncio_data.get("precio", 0)
        
        # 1. Score base por contexto vehicular
        score_contexto = self._score_contexto_vehicular(texto, modelo)
        score += score_contexto
        if score_contexto > 0:
            razones.append(f"contexto_vehicular_{score_contexto}")
        
        # 2. Score por validación de precio
        precio_valido, _ = validar_precio_coherente(precio, modelo, anio)
        if precio_valido:
            score += 20
            razones.append("precio_coherente")
        else:
            score += PENALTY_INVALID
            razones.append("precio_invalido")
        
        # 3. Evaluación ROI y oportunidad
        roi_data = calcular_roi_real(modelo, precio, anio)
        roi_valor = roi_data.get("roi", 0)
        
        if roi_valor >= ROI_MINIMO:
            score += BONUS_ROI_EXCELENTE
            razones.append(f"roi_excelente_{roi_valor}")
        elif roi_valor >= 5:
            score += BONUS_ROI_BUENO
            razones.append(f"roi_bueno_{roi_valor}")
        elif roi_valor > 0:
            score += 8
            razones.append(f"roi_positivo_{roi_valor}")
        else:
            score -= 5
            razones.append(f"roi_bajo_{roi_valor}")
        
        # 4. Bonificación por confianza estadística
        confianza = roi_data.get("confianza", "baja")
        muestra = roi_data.get("muestra", 0)
        
        if confianza == "alta":
            score += 15
            razones.append(f"confianza_alta_muestra_{muestra}")
        elif confianza == "media":
            score += 8
            razones.append(f"confianza_media_muestra_{muestra}")
        else:
            score -= 3
            razones.append("confianza_baja")
        
        # 5. Penalizaciones por contexto negativo
        es_critico, pen_negativa = evaluar_contexto_negativo(texto)
        if not es_critico:  # Ya se manejó en short-circuits
            score += pen_negativa
            if pen_negativa < 0:
                razones.append(f"contexto_negativo_leve_{pen_negativa}")
        
        # 6. Penalización por ubicación extranjera
        if es_extranjero(texto):
            score -= 10
            razones.append("ubicacion_extranjera")
        
        # 7. Bonificaciones adicionales
        bonus_extra = self._calcular_bonus_extra(texto, modelo, anio, precio)
        score += bonus_extra
        if bonus_extra > 0:
            razones.append(f"bonus_extra_{bonus_extra}")
        
        # Resultado final
        es_relevante = (score >= self.threshold_relevante and 
                       roi_valor >= (ROI_MINIMO - 2) and 
                       precio_valido)
        
        return {
            "score": score,
            "descartado": score <= self.threshold_descarte,
            "relevante": es_relevante,
            "razones": razones,
            "roi_data": roi_data,
            "anio": anio,
            "razon_descarte": "score_insuficiente" if score <= self.threshold_descarte else None
        }
    
    def _score_contexto_vehicular(self, texto: str, modelo: str) -> int:
        """Score inteligente basado en contexto vehicular"""
        score = 0
        texto_lower = texto.lower()
        
        # Bonus fuerte por modelo detectado
        if modelo and modelo.lower() in texto_lower:
            score += 25
        
        # Patrones vehiculares muy fuertes (+15 cada uno)
        patrones_muy_fuertes = [
            r"\b(modelo|año|del año|versión|m/)\b",
            r"\b(vendo|se vende|en venta|ofrezco)\b"
        ]
        
        for patron in patrones_muy_fuertes:
            if re.search(patron, texto, re.IGNORECASE):
                score += 15
        
        # Patrones vehiculares fuertes (+10 cada uno)
        patrones_fuertes = [
            r"\b(toyota|honda|nissan|ford|chevrolet|hyundai|kia|mazda|mitsubishi|suzuki)\b",
            r"\b(sedan|hatchback|suv|pickup|camioneta)\b",
            r"\b(motor|transmisión|automático|standard|mecánico)\b"
        ]
        
        for patron in patrones_fuertes:
            if re.search(patron, texto, re.IGNORECASE):
                score += 10
        
        # Patrones vehiculares moderados (+5 cada uno)
        patrones_moderados = [
            r"\b(kilometraje|km|millas|gasolina|diesel)\b",
            r"\b(papeles|documentos|traspaso|placas)\b",
            r"\b(llantas|rines|asientos|aire acondicionado)\b",
            r"\b(excelente estado|impecable|bien cuidado)\b"
        ]
        
        for patron in patrones_moderados:
            if re.search(patron, texto, re.IGNORECASE):
                score += 5
        
        return min(score, 80)  # Cap máximo
    
    def _calcular_bonus_extra(self, texto: str, modelo: str, anio: int, precio: int) -> int:
        """Bonificaciones adicionales para compensar el balanceo"""
        bonus = 0
        
        # Bonus por año reciente
        if anio >= (CURRENT_YEAR - 8):
            bonus += 10
        
        # Bonus por precio en rango común de mercado
        if 10000 <= precio <= 200000:
            bonus += 8
        
        # Bonus por texto detallado
        if len(texto) > 150:
            bonus += 6
        
        # Bonus por palabras positivas
        palabras_positivas = ["excelente", "impecable", "full", "equipado", "mantenimiento", "cuidado"]
        for palabra in palabras_positivas:
            if palabra in texto.lower():
                bonus += 4
                break
        
        # Bonus por información específica
        if any(info in texto.lower() for info in ["km", "kilometraje", "papeles", "documentos"]):
            bonus += 5
        
        return bonus

# Instancia global del scoring engine
_scoring_engine = None

def get_scoring_engine():
    """Singleton para ScoringEngine"""
    global _scoring_engine
    if _scoring_engine is None:
        _scoring_engine = ScoringEngine()
    return _scoring_engine

# ============================================================================
# FUNCIONES DE BASE DE DATOS Y PERSISTENCIA
# ============================================================================
@timeit
def inicializar_tabla_anuncios():
    """Inicializa la tabla de anuncios con todas las columnas necesarias"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Verificar si la tabla existe
        cur.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='anuncios'
        """)
        tabla_existe = cur.fetchone() is not None
        
        if not tabla_existe:
            # Crear tabla con estructura completa
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
                    relevante BOOLEAN DEFAULT 0,
                    confianza_precio TEXT DEFAULT 'baja',
                    muestra_precio INTEGER DEFAULT 0
                )
            """)
            print("✅ Tabla anuncios creada con estructura completa")
        else:
            # Verificar y agregar columnas faltantes
            cur.execute("PRAGMA table_info(anuncios)")
            columnas_existentes = {row[1] for row in cur.fetchall()}
            
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

@timeit
def get_precio_referencia(modelo: str, anio: int, tolerancia: Optional[int] = None) -> Dict[str, Any]:
    """Obtiene precio de referencia para un modelo y año específicos"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT precio FROM anuncios 
            WHERE modelo=? AND ABS(anio - ?) <= ? AND precio > 0
            ORDER BY precio
        """, (modelo, anio, tolerancia or TOLERANCIA_PRECIO_REF))
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

def filtrar_outliers(precios: List[int]) -> List[int]:
    """Filtra valores atípicos usando método IQR"""
    if len(precios) < 4:
        return precios
    try:
        q1, q3 = statistics.quantiles(precios, n=4)[0], statistics.quantiles(precios, n=4)[2]
        iqr = q3 - q1
        lim_inf = q1 - 2.0 * iqr
        lim_sup = q3 + 2.0 * iqr
        filtrados = [p for p in precios if lim_inf <= p <= lim_sup]
        return filtrados if len(filtrados) >= 2 else precios
    except:
        return precios

@timeit
def calcular_roi_real(modelo: str, precio_compra: int, anio: int, costo_extra: int = 2000) -> Dict[str, Any]:
    """Calcula ROI real basado en depreciación y precios de mercado"""
    ref = get_precio_referencia(modelo, anio)
    años_antiguedad = max(0, datetime.now().year - anio)
    factor_depreciacion = (1 - DEPRECIACION_ANUAL) ** años_antiguedad
    precio_depreciado = ref["precio"] * factor_depreciacion
    inversion_total = precio_compra + costo_extra
    roi = ((precio_depreciado - inversion_total) / inversion_total) * 100 if inversion_total > 0 else 0.0
    
    return {
        "roi": round(roi, 1),
        "precio_referencia": ref["precio"],
        "precio_depreciado": int(precio_depreciado),
        "confianza": ref["confianza"],
        "muestra": ref["muestra"],
        "inversion_total": inversion_total,
        "años_antiguedad": años_antiguedad
    }

@timeit
def insertar_anuncio_db(link, modelo, anio, precio, km, roi, score, relevante=False,
                        confianza_precio=None, muestra_precio=None):
    """Inserta o actualiza un anuncio en la base de datos"""
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
    INSERT OR REPLACE INTO anuncios 
    (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio, fecha_scrape)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE('now'))
    """, (link, modelo, anio, precio, km, roi, score, relevante, confianza_precio, muestra_precio))
    
    conn.commit()

def existe_en_db(link: str) -> bool:
    """Verifica si un anuncio ya existe en la base de datos"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM anuncios WHERE link = ?", (limpiar_link(link),))
        return cur.fetchone() is not None

def existe_en_cache(link: str, modelo: str) -> bool:
    """Verifica existencia usando el cache en memoria (MUCHO más rápido)"""
    existentes = _cache_anuncios.get_existentes(modelo)
    return limpiar_link(link) in existentes

# ============================================================================
# FUNCIONES PRINCIPALES DE ANÁLISIS Y PROCESAMIENTO
# ============================================================================
def analizar_mensaje(texto: str) -> Optional[Dict[str, Any]]:
    """
    INTERFAZ PRINCIPAL - Mantiene compatibilidad con versiones anteriores
    Analiza un mensaje/anuncio y extrae información relevante
    """
    # Preprocesamiento
    texto = limpiar_emojis_numericos(texto)
    texto = normalizar_formatos_ano(texto)
    
    # Extracción básica
    precio = limpiar_precio(texto)
    modelo = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
    anio = extraer_anio(texto, modelo, precio, debug=DEBUG)
    
    # Validación básica
    if not (modelo and anio and precio):
        if DEBUG:
            print(f"❌ Datos insuficientes: modelo={modelo}, anio={anio}, precio={precio}")
        return None
    
    # Validación de precio
    precio_valido, razon = validar_precio_coherente(precio, modelo, anio)
    if not precio_valido:
        if DEBUG:
            print(f"❌ Precio inválido: {razon}")
        return None
    
    # Calcular ROI y score usando el motor inteligente
    roi_data = calcular_roi_real(modelo, precio, anio)
    
    # Usar scoring engine para evaluación completa
    engine = get_scoring_engine()
    resultado_scoring = engine.evaluar_anuncio_completo({
        "titulo": texto,
        "modelo": modelo,
        "anio": anio,
        "precio": precio
    })
    
    score = resultado_scoring.get("score", 0)
    
    # Extraer URL si existe
    url = next((l for l in texto.split() if l.startswith("http")), "")
    
    # Construir respuesta manteniendo interfaz original
    return {
        "url": limpiar_link(url),
        "modelo": modelo,
        "año": anio,  # Mantener "año" para compatibilidad
        "precio": precio,
        "roi": roi_data["roi"],
        "score": score,
        "relevante": resultado_scoring.get("relevante", False),
        "km": "",  # Campo mantenido por compatibilidad
        "confianza_precio": roi_data["confianza"],
        "muestra_precio": roi_data["muestra"],
        "roi_data": roi_data
    }

def procesar_anuncios_batch(anuncios: List[Dict[str, Any]], modelo: str) -> List[Dict[str, Any]]:
    """
    FUNCIÓN OPTIMIZADA PARA PROCESAMIENTO EN LOTE
    Utiliza cache en memoria y short-circuits para máximo rendimiento
    """
    if not anuncios:
        return []
    
    # Obtener existentes una sola vez para todo el lote
    existentes = _cache_anuncios.get_existentes(modelo)
    engine = get_scoring_engine()
    
    resultados = []
    procesados = 0
    descartados = 0
    
    if DEBUG:
        print(f"🚀 Procesando {len(anuncios)} anuncios de {modelo}")
        start_time = time.time()
    
    for anuncio in anuncios:
        # Preprocesar datos del anuncio
        anuncio_data = {
            "titulo": anuncio.get("titulo", ""),
            "precio": limpiar_precio(anuncio.get("precio", "") or anuncio.get("titulo", "")),
            "url": anuncio.get("url", ""),
            "modelo": modelo
        }
        
        # Evaluación rápida con short-circuits
        resultado = engine.evaluar_anuncio_rapido(anuncio_data, existentes)
        
        if resultado["descartado"]:
            descartados += 1
            if DEBUG and descartados <= 5:  # Solo mostrar primeros 5 descartes
                print(f"⚠️ Descartado: {resultado['razon_descarte']}")
            continue
        
        # Si pasó la evaluación rápida, completar datos
        anio = resultado.get("anio")
        if anio:
            roi_data = calcular_roi_real(modelo, anuncio_data["precio"], anio)
            
            resultado_final = {
                "url": limpiar_link(anuncio_data["url"]),
                "modelo": modelo,
                "año": anio,
                "precio": anuncio_data["precio"],
                "roi": roi_data["roi"],
                "score": resultado["score"],
                "relevante": resultado["relevante"],
                "km": anuncio.get("km", ""),
                "confianza_precio": roi_data["confianza"],
                "muestra_precio": roi_data["muestra"],
                "roi_data": roi_data
            }
            
            resultados.append(resultado_final)
            procesados += 1
    
    if DEBUG:
        elapsed = time.time() - start_time
        print(f"✅ Procesamiento completado en {elapsed:.2f}s:")
        print(f"   - Procesados: {procesados}")
        print(f"   - Descartados: {descartados}")
        print(f"   - Rate: {len(anuncios)/elapsed:.1f} anuncios/segundo")
    
    return resultados

# ============================================================================
# FUNCIONES DE COMPATIBILIDAD Y UTILIDADES
# ============================================================================
def puntuar_anuncio(anuncio: Dict[str, Any]) -> int:
    """
    FUNCIÓN DE COMPATIBILIDAD - Mantiene interfaz original
    """
    engine = get_scoring_engine()
    resultado = engine.evaluar_anuncio_completo(anuncio)
    return resultado.get("score", 0)

def calcular_score(año: int, contexto: str, fuente: str, precio: Optional[int] = None) -> int:
    """
    FUNCIÓN DE COMPATIBILIDAD - Mantiene interfaz original del v1
    """
    # Mapear fuente a peso base
    if fuente == 'modelo':
        score_base = WEIGHT_MODEL
    elif fuente == 'titulo':
        score_base = WEIGHT_TITLE
    elif fuente == 'ventana':
        score_base = WEIGHT_WINDOW
    else:
        score_base = WEIGHT_GENERAL
    
    # Ajustes por contexto (versión simplificada)
    if any(palabra in contexto.lower() for palabra in ["modelo", "año", "vendo", "auto"]):
        score_base += BONUS_VEHICULO
    
    if precio and 5000 <= precio <= 300000:
        score_base += BONUS_PRECIO_HIGH
    
    return score_base

@timeit
def get_rendimiento_modelo(modelo: str, dias: int = 7) -> float:
    """Obtiene el rendimiento de detección para un modelo específico"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT SUM(CASE WHEN score >= ? THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
            FROM anuncios WHERE modelo = ? AND fecha_scrape >= date('now', ?)
        """, (SCORE_MIN_DB, modelo, f"-{dias} days"))
        result = cur.fetchone()[0]
        return round(result or 0.0, 3)

def modelos_bajo_rendimiento(threshold: float = 0.01, dias: int = 7) -> List[str]:
    """Identifica modelos con bajo rendimiento de detección"""
    return [m for m in MODELOS_INTERES if get_rendimiento_modelo(m, dias) < threshold]

def get_estadisticas_db() -> Dict[str, Any]:
    """Obtiene estadísticas generales de la base de datos"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM anuncios")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'alta'")
        alta_conf = cur.fetchone()[0] or 0
        
        cur.execute("SELECT COUNT(*) FROM anuncios WHERE confianza_precio = 'baja'")
        baja_conf = cur.fetchone()[0] or 0
        
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
    """Obtiene un anuncio específico de la base de datos"""
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
    """Compara si dos anuncios son diferentes en campos clave"""
    campos_clave = ["modelo", "anio", "precio", "km", "roi", "score"]
    return any(str(a.get(c)) != str(b.get(c)) for c in campos_clave)

# ============================================================================
# FUNCIONES DE DEBUGGING Y TESTING
# ============================================================================
def debug_extraccion_anio(texto: str, modelo: str = None) -> None:
    """Función de debugging para la extracción de años"""
    print(f"\n🔍 DEBUG: Extracción de año")
    print(f"Texto: {texto[:100]}...")
    print(f"Modelo: {modelo}")
    print("=" * 50)
    
    anio = extraer_anio(texto, modelo, debug=True)
    print(f"\n✅ Resultado final: {anio}")

def test_scoring_completo(texto: str, modelo: str = None) -> None:
    """Función de testing completa del sistema de scoring"""
    print(f"\n🧪 TEST COMPLETO DEL SISTEMA")
    print(f"Texto: {texto[:80]}...")
    print("=" * 60)
    
    # Test de análisis completo
    resultado = analizar_mensaje(texto)
    
    if resultado:
        print(f"✅ ANÁLISIS EXITOSO:")
        print(f"   Modelo: {resultado['modelo']}")
        print(f"   Año: {resultado['año']}")
        print(f"   Precio: Q{resultado['precio']:,}")
        print(f"   ROI: {resultado['roi']:.1f}%")
        print(f"   Score: {resultado['score']}")
        print(f"   Relevante: {resultado['relevante']}")
        print(f"   Confianza: {resultado['confianza_precio']}")
    else:
        print("❌ ANÁLISIS FALLÓ - Anuncio descartado")
        
        # Debug paso a paso para ver dónde falló
        precio = limpiar_precio(texto)
        modelo_det = next((m for m in MODELOS_INTERES if coincide_modelo(texto, m)), None)
        anio = extraer_anio(texto, modelo_det, precio)
        
        print(f"\n🔍 DEBUG PASO A PASO:")
        print(f"   Precio extraído: {precio}")
        print(f"   Modelo detectado: {modelo_det}")
        print(f"   Año extraído: {anio}")
        
        if modelo_det and anio and precio:
            precio_valido, razon = validar_precio_coherente(precio, modelo_det, anio)
            print(f"   Precio válido: {precio_valido} ({razon})")

def test_rendimiento_batch():
    """Test de rendimiento del procesamiento en lote"""
    anuncios_test = [
        {"titulo": "Vendo Toyota Yaris 2015 Q25000", "url": "http://test1.com"},
        {"titulo": "Honda Civic 2018 excelente estado Q45000", "url": "http://test2.com"},
        {"titulo": "Nissan Sentra 2012 Q18000 papeles al día", "url": "http://test3.com"},
    ] * 100  # 300 anuncios de prueba
    
    start_time = time.time()
    resultados = procesar_anuncios_batch(anuncios_test, "yaris")
    elapsed = time.time() - start_time
    
    print(f"\n🚀 TEST DE RENDIMIENTO:")
    print(f"   Anuncios procesados: {len(anuncios_test)}")
    print(f"   Tiempo total: {elapsed:.3f}s")
    print(f"   Rate: {len(anuncios_test)/elapsed:.1f} anuncios/segundo")
    print(f"   Resultados válidos: {len(resultados)}")

# ============================================================================
# INICIALIZACIÓN Y CONFIGURACIÓN
# ============================================================================
def inicializar_sistema():
    """Inicializa todos los componentes del sistema"""
    print("🚀 Inicializando sistema optimizado...")
    
    # Inicializar base de datos
    inicializar_tabla_anuncios()
    
    # Inicializar scoring engine
    engine = get_scoring_engine()
    
    # Limpiar cache
    _cache_anuncios.invalidar()
    
    print("✅ Sistema inicializado correctamente")
    print(f"   - Modelos soportados: {len(MODELOS_INTERES)}")
    print(f"   - Score mínimo DB: {SCORE_MIN_DB}")
    print(f"   - Score mínimo Telegram: {SCORE_MIN_TELEGRAM}")
    print(f"   - ROI mínimo: {ROI_MINIMO}%")

# Inicialización automática al importar el módulo
if __name__ == "__main__":
    inicializar_sistema()
    
    # Ejecutar tests si se corre directamente
    print("\n🧪 Ejecutando tests...")
    test_scoring_completo("Vendo Toyota Yaris 2015 Q25000 excelente estado")
    test_rendimiento_batch()
