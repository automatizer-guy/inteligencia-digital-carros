"""
correcciones.py - Versión Inteligente con Detección de Patrones

Este archivo reemplaza completamente el correcciones.py original,
manteniendo compatibilidad total pero añadiendo inteligencia de patrones.
"""

import json
import os
import re
from typing import Optional, Dict, List
from datetime import datetime

# Importar el detector inteligente
try:
    from detector_inteligente import DetectorAñoInteligente
    DETECTOR_DISPONIBLE = True
except ImportError:
    DETECTOR_DISPONIBLE = False
    print("⚠️ Detector inteligente no disponible, usando sistema básico")

CORRECCIONES_FILE = "correcciones.json"

# Instancia global del detector inteligente
_detector_global = None

def _get_detector():
    """Obtiene instancia del detector inteligente (singleton)"""
    global _detector_global
    if _detector_global is None and DETECTOR_DISPONIBLE:
        _detector_global = DetectorAñoInteligente(CORRECCIONES_FILE)
    return _detector_global

def cargar_correcciones():
    """
    Carga las correcciones desde el archivo JSON
    🔄 COMPATIBLE: Mantiene la función original para retrocompatibilidad
    """
    if not os.path.exists(CORRECCIONES_FILE):
        return {}
    try:
        with open(CORRECCIONES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        print("⚠️ Error al cargar correcciones.json, creando archivo nuevo")
        return {}

def normalizar_texto_correccion(texto: str) -> str:
    """
    Normaliza el texto para búsqueda de correcciones más flexible
    🔄 COMPATIBLE: Función original mantenida
    """
    texto = texto.strip().lower()
    texto = re.sub(r'[🔥✅💥🚘🔰⚠️🥶]', '', texto)
    texto = re.sub(r'\s+', ' ', texto)
    texto = re.sub(r'[.,!?]+$', '', texto)
    return texto.strip()

def guardar_correccion(texto: str, año: int):
    """
    Guarda una nueva corrección y re-entrena el sistema inteligente
    ✨ MEJORADO: Ahora re-aprende patrones automáticamente
    """
    detector = _get_detector()
    
    if detector:
        # Usar sistema inteligente que re-aprende automáticamente
        detector.agregar_correccion_y_reaprender(texto, año)
    else:
        # Fallback al sistema original
        correcciones = cargar_correcciones()
        texto_normalizado = normalizar_texto_correccion(texto)
        correcciones[texto_normalizado] = año
        
        try:
            with open(CORRECCIONES_FILE, "w", encoding="utf-8") as f:
                json.dump(correcciones, f, indent=2, ensure_ascii=False)
            print(f"✅ Corrección guardada: '{texto[:50]}...' → {año}")
        except Exception as e:
            print(f"❌ Error al guardar corrección: {e}")

def obtener_correccion(texto: str, debug: bool = False) -> Optional[int]:
    """
    🚀 FUNCIÓN PRINCIPAL: Busca corrección usando sistema inteligente
    
    PRIORIDAD:
    1. Coincidencias exactas
    2. Patrones aprendidos automáticamente  
    3. Búsqueda parcial mejorada
    
    Args:
        texto: Texto a analizar
        debug: Si mostrar información de depuración
        
    Returns:
        Año detectado o None si no se encuentra
    """
    detector = _get_detector()
    
    if detector:
        # 🧠 USAR SISTEMA INTELIGENTE
        resultado = detector.detectar_año_inteligente(texto, debug)
        
        if debug and resultado:
            print(f"🎯 Sistema inteligente detectó: {resultado}")
        
        return resultado
    else:
        # 📋 FALLBACK: Sistema original básico
        return _obtener_correccion_basico(texto, debug)

def _obtener_correccion_basico(texto: str, debug: bool = False) -> Optional[int]:
    """
    Sistema básico original como fallback
    """
    correcciones = cargar_correcciones()
    if not correcciones:
        return None
    
    texto_normalizado = normalizar_texto_correccion(texto)
    
    # 1. Búsqueda exacta
    if texto_normalizado in correcciones:
        if debug:
            print(f"✅ Coincidencia exacta: {correcciones[texto_normalizado]}")
        return correcciones[texto_normalizado]
    
    # 2. Búsqueda parcial básica
    texto_palabras = set(texto_normalizado.split())
    mejor_coincidencia = None
    mejor_score = 0
    
    for correccion_texto, año in correcciones.items():
        correccion_palabras = set(correccion_texto.split())
        
        if len(correccion_palabras) > 0:
            palabras_comunes = texto_palabras.intersection(correccion_palabras)
            score = len(palabras_comunes) / len(correccion_palabras)
            
            if score >= 0.7 and score > mejor_score:
                palabras_clave = {'toyota', 'honda', 'nissan', 'suzuki', 'hyundai', 
                                'civic', 'yaris', 'sentra', 'crv', 'cr-v', 'rav4', 
                                'accent', 'swift', 'alto'}
                if palabras_clave.intersection(correccion_palabras):
                    mejor_coincidencia = año
                    mejor_score = score
    
    if debug and mejor_coincidencia:
        print(f"🔍 Búsqueda parcial encontró: {mejor_coincidencia} (score: {mejor_score:.2f})")
    
    return mejor_coincidencia

def listar_correcciones() -> Dict[str, int]:
    """
    Lista todas las correcciones disponibles
    🔄 COMPATIBLE: Función original mantenida
    """
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
    """
    Muestra estadísticas completas del sistema
    ✨ MEJORADO: Ahora incluye estadísticas del sistema inteligente
    """
    detector = _get_detector()
    
    if detector:
        # Mostrar estadísticas del sistema inteligente
        detector.estadisticas_sistema()
    else:
        # Estadísticas básicas originales
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
    """
    Limpia correcciones duplicadas o muy similares
    🔄 COMPATIBLE: Función original mantenida
    """
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
        
        # Si hay detector inteligente, recargar y re-aprender
        detector = _get_detector()
        if detector:
            detector.cargar_y_aprender()
            print("🧠 Patrones re-aprendidos con correcciones limpias")
        
    except Exception as e:
        print(f"❌ Error al limpiar correcciones: {e}")

def test_sistema_inteligente():
    """
    🧪 FUNCIÓN DE TESTING: Prueba el sistema inteligente
    """
    print("🧪 PROBANDO SISTEMA INTELIGENTE DE DETECCIÓN")
    print("="*50)
    
    casos_prueba = [
        "Toyota yaris modelo 09",
        "Honda civic modelo 03", 
        "Suzuki swift año 2011",
        "Toyota yaris del 2012",
        "Honda accord 2015",
        "Nissan sentra modelo 05 activo",
        "Vendo toyota corolla modelo 08",
        "Hyundai accent modelo 14 automático"
    ]
    
    aciertos = 0
    for i, caso in enumerate(casos_prueba, 1):
        print(f"\n📱 CASO {i}: '{caso}'")
        resultado = obtener_correccion(caso, debug=True)
        
        if resultado:
            print(f"  ✅ DETECTADO: {resultado}")
            aciertos += 1
        else:
            print(f"  ❌ No detectado")
    
    print(f"\n📊 RESULTADO: {aciertos}/{len(casos_prueba)} casos exitosos")
    print(f"📈 Tasa de éxito: {aciertos/len(casos_prueba)*100:.1f}%")

# FUNCIÓN PRINCIPAL PARA COMPATIBILIDAD TOTAL
def main():
    """Función principal para testing"""
    detector = _get_detector()
    
    if detector:
        print("🚀 Sistema inteligente cargado exitosamente")
        test_sistema_inteligente()
    else:
        print("📋 Usando sistema básico (detector inteligente no disponible)")

if __name__ == "__main__":
    main()
