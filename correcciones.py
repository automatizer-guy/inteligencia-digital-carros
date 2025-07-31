import json
import os
import re
from typing import Optional, Dict

CORRECCIONES_FILE = "correcciones.json"

def cargar_correcciones():
    """Carga las correcciones desde el archivo JSON"""
    if not os.path.exists(CORRECCIONES_FILE):
        return {}
    try:
        with open(CORRECCIONES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        print("⚠️ Error al cargar correcciones.json, creando archivo nuevo")
        return {}

def normalizar_texto_correccion(texto: str) -> str:
    """Normaliza el texto para búsqueda de correcciones más flexible"""
    # Convertir a minúsculas
    texto = texto.strip().lower()
    
    # Remover caracteres especiales y emojis comunes
    texto = re.sub(r'[🔥✅💥🚘🔰⚠️🥶]', '', texto)
    
    # Normalizar espacios múltiples
    texto = re.sub(r'\s+', ' ', texto)
    
    # Remover puntuación al final
    texto = re.sub(r'[.,!?]+$', '', texto)
    
    return texto.strip()

def guardar_correccion(texto: str, año: int):
    """Guarda una nueva corrección"""
    correcciones = cargar_correcciones()
    texto_normalizado = normalizar_texto_correccion(texto)
    correcciones[texto_normalizado] = año
    
    try:
        with open(CORRECCIONES_FILE, "w", encoding="utf-8") as f:
            json.dump(correcciones, f, indent=2, ensure_ascii=False)
        print(f"✅ Corrección guardada: '{texto[:50]}...' → {año}")
    except Exception as e:
        print(f"❌ Error al guardar corrección: {e}")

def obtener_correccion(texto: str) -> Optional[int]:
    """
    Busca una corrección para el texto dado.
    Primero búsqueda exacta, luego búsqueda parcial.
    """
    correcciones = cargar_correcciones()
    if not correcciones:
        return None
    
    texto_normalizado = normalizar_texto_correccion(texto)
    
    # 1. Búsqueda exacta
    if texto_normalizado in correcciones:
        return correcciones[texto_normalizado]
    
    # 2. Búsqueda parcial - buscar si alguna corrección está contenida en el texto
    texto_palabras = set(texto_normalizado.split())
    mejor_coincidencia = None
    mejor_score = 0
    
    for correccion_texto, año in correcciones.items():
        correccion_palabras = set(correccion_texto.split())
        
        # Calcular intersección de palabras
        palabras_comunes = texto_palabras.intersection(correccion_palabras)
        
        # Score basado en porcentaje de palabras coincidentes
        if len(correccion_palabras) > 0:
            score = len(palabras_comunes) / len(correccion_palabras)
            
            # Requerir al menos 70% de coincidencia
            if score >= 0.7 and score > mejor_score:
                # Verificar que tenga palabras clave importantes (modelo de auto)
                palabras_clave = {'toyota', 'honda', 'nissan', 'suzuki', 'hyundai', 'civic', 'yaris', 'sentra', 'crv', 'cr-v', 'rav4', 'accent', 'swift', 'alto'}
                if palabras_clave.intersection(correccion_palabras):
                    mejor_coincidencia = año
                    mejor_score = score
    
    return mejor_coincidencia

def listar_correcciones() -> Dict[str, int]:
    """Lista todas las correcciones disponibles"""
    correcciones = cargar_correcciones()
    print(f"📝 Total de correcciones: {len(correcciones)}")
    
    # Agrupar por año para mejor visualización
    por_año = {}
    for texto, año in correcciones.items():
        if año not in por_año:
            por_año[año] = []
        por_año[año].append(texto)
    
    for año in sorted(por_año.keys()):
        textos = por_año[año]
        print(f"\n🗓️ Año {año} ({len(textos)} correcciones):")
        for texto in sorted(textos)[:3]:  # Mostrar solo las primeras 3
            print(f"  - {texto[:60]}...")
        if len(textos) > 3:
            print(f"  ... y {len(textos) - 3} más")
    
    return correcciones

def estadisticas_correcciones():
    """Muestra estadísticas de las correcciones"""
    correcciones = cargar_correcciones()
    
    if not correcciones:
        print("📊 No hay correcciones guardadas")
        return
    
    # Contar por décadas
    por_decada = {}
    for año in correcciones.values():
        decada = (año // 10) * 10
        por_decada[decada] = por_decada.get(decada, 0) + 1
    
    print("📊 Estadísticas de correcciones:")
    print(f"  Total: {len(correcciones)}")
    print("  Por década:")
    for decada in sorted(por_decada.keys()):
        print(f"    {decada}s: {por_decada[decada]} correcciones")
    
    # Años más comunes
    años_comunes = {}
    for año in correcciones.values():
        años_comunes[año] = años_comunes.get(año, 0) + 1
    
    print("  Años más frecuentes:")
    for año, count in sorted(años_comunes.items(), key=lambda x: -x[1])[:5]:
        print(f"    {año}: {count} correcciones")

def limpiar_correcciones_duplicadas():
    """Limpia correcciones duplicadas o muy similares"""
    correcciones = cargar_correcciones()
    original_count = len(correcciones)
    
    # Agrupar por año y encontrar textos muy similares
    por_año = {}
    for texto, año in correcciones.items():
        if año not in por_año:
            por_año[año] = []
        por_año[año].append(texto)
    
    correcciones_limpias = {}
    
    for año, textos in por_año.items():
        textos_únicos = []
        
        for texto in textos:
            # Verificar si es muy similar a algún texto ya guardado
            es_similar = False
            for texto_único in textos_únicos:
                # Calcular similitud básica
                palabras1 = set(texto.split())
                palabras2 = set(texto_único.split())
                intersection = len(palabras1.intersection(palabras2))
                union = len(palabras1.union(palabras2))
                similitud = intersection / union if union > 0 else 0
                
                if similitud > 0.8:  # 80% de similitud
                    es_similar = True
                    break
            
            if not es_similar:
                textos_únicos.append(texto)
                correcciones_limpias[texto] = año
    
    # Guardar correcciones limpias
    try:
        with open(CORRECCIONES_FILE, "w", encoding="utf-8") as f:
            json.dump(correcciones_limpias, f, indent=2, ensure_ascii=False)
        
        print(f"🧹 Limpieza completada:")
        print(f"  - Antes: {original_count} correcciones")
        print(f"  - Después: {len(correcciones_limpias)} correcciones")
        print(f"  - Eliminadas: {original_count - len(correcciones_limpias)} duplicadas")
        
    except Exception as e:
        print(f"❌ Error al limpiar correcciones: {e}")
