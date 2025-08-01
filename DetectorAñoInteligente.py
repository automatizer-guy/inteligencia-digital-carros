import re
import json
from typing import Optional, List, Dict, Tuple
from datetime import datetime

class DetectorAñoInteligente:
    """
    Sistema inteligente que aprende patrones de las correcciones manuales
    """
    
    def __init__(self):
        self.correcciones = {}  # Correcciones exactas originales
        self.patrones_aprendidos = {}  # Patrones extraídos automáticamente
        
    def cargar_correcciones(self, archivo_correcciones: str):
        """Carga correcciones y extrae patrones automáticamente"""
        try:
            with open(archivo_correcciones, 'r', encoding='utf-8') as f:
                self.correcciones = json.load(f)
            
            # Extraer patrones de las correcciones existentes
            self._extraer_patrones_automaticamente()
            
        except FileNotFoundError:
            self.correcciones = {}
            self.patrones_aprendidos = {}
    
    def _extraer_patrones_automaticamente(self):
        """
        ✨ MAGIA: Extrae patrones automáticamente de las correcciones
        """
        print("🧠 Extrayendo patrones de correcciones existentes...")
        
        patrones_por_modelo = {}
        
        for texto_correccion, año in self.correcciones.items():
            # Identificar modelo en el texto
            modelo = self._identificar_modelo(texto_correccion)
            if not modelo:
                continue
                
            if modelo not in patrones_por_modelo:
                patrones_por_modelo[modelo] = []
            
            # Extraer contexto del año en la corrección
            patron = self._extraer_patron_contexto(texto_correccion, año)
            if patron:
                patrones_por_modelo[modelo].append({
                    'patron': patron,
                    'año_ejemplo': año,
                    'texto_original': texto_correccion
                })
        
        # Generar patrones inteligentes
        for modelo, datos in patrones_por_modelo.items():
            self.patrones_aprendidos[modelo] = self._generar_patrones_modelo(datos)
        
        self._mostrar_patrones_aprendidos()
    
    def _identificar_modelo(self, texto: str) -> Optional[str]:
        """Identifica qué modelo de auto está en el texto"""
        modelos = ['yaris', 'civic', 'corolla', 'sentra', 'cr-v', 'crv', 'rav4', 'accent', 'swift', 'alto']
        
        for modelo in modelos:
            if modelo in texto.lower():
                return modelo
        return None
    
    def _extraer_patron_contexto(self, texto: str, año: int) -> Optional[Dict]:
        """
        Extrae el patrón de cómo aparece el año en el contexto
        """
        año_str = str(año)
        año_corto = str(año)[2:]  # 07, 08, 09, etc.
        
        # Buscar diferentes formatos del año en el texto
        patrones_encontrados = []
        
        # Patrón 1: "modelo XX" donde XX es año corto
        if re.search(rf'modelo\s+{año_corto}\b', texto, re.IGNORECASE):
            patrones_encontrados.append({
                'tipo': 'modelo_año_corto',
                'formato': f'modelo {año_corto}',
                'año_completo': año,
                'año_corto': año_corto
            })
        
        # Patrón 2: "del YYYY" donde YYYY es año completo
        if re.search(rf'del\s+{año_str}\b', texto, re.IGNORECASE):
            patrones_encontrados.append({
                'tipo': 'del_año_completo', 
                'formato': f'del {año_str}',
                'año_completo': año,
                'año_corto': año_corto
            })
        
        # Patrón 3: "año YYYY"
        if re.search(rf'año\s+{año_str}\b', texto, re.IGNORECASE):
            patrones_encontrados.append({
                'tipo': 'año_completo',
                'formato': f'año {año_str}',
                'año_completo': año,
                'año_corto': año_corto
            })
        
        # Patrón 4: Solo año corto al final
        if re.search(rf'\b{año_corto}$', texto):
            patrones_encontrados.append({
                'tipo': 'año_corto_final',
                'formato': año_corto,
                'año_completo': año,
                'año_corto': año_corto
            })
        
        return patrones_encontrados[0] if patrones_encontrados else None
    
    def _generar_patrones_modelo(self, datos_modelo: List[Dict]) -> Dict:
        """
        Genera patrones inteligentes para un modelo específico
        """
        patrones = {
            'modelo_año_corto': [],
            'del_año_completo': [],
            'año_completo': [],
            'año_corto_final': []
        }
        
        for dato in datos_modelo:
            if dato['patron']:
                tipo = dato['patron']['tipo']
                if tipo in patrones:
                    patrones[tipo].append(dato['patron'])
        
        return patrones
    
    def detectar_año_inteligente(self, texto: str, debug: bool = False) -> Optional[int]:
        """
        🚀 DETECCIÓN INTELIGENTE: Combina correcciones exactas + patrones aprendidos
        """
        texto_original = texto
        texto_norm = self._normalizar_texto(texto)
        
        if debug:
            print(f"🔍 Analizando: '{texto[:50]}...'")
        
        # 1. Búsqueda exacta en correcciones (como antes)
        coincidencia_exacta = self._busqueda_exacta(texto_norm)
        if coincidencia_exacta:
            if debug:
                print(f"✅ Coincidencia exacta: {coincidencia_exacta}")
            return coincidencia_exacta
        
        # 2. 🧠 NUEVO: Búsqueda por patrones aprendidos
        modelo = self._identificar_modelo(texto)
        if modelo and modelo in self.patrones_aprendidos:
            año_patron = self._aplicar_patrones_modelo(texto, modelo, debug)
            if año_patron:
                if debug:
                    print(f"🎯 Detectado por patrón: {año_patron}")
                return año_patron
        
        # 3. Búsqueda parcial mejorada (como propuse antes)
        año_parcial = self._busqueda_parcial(texto_norm, debug)
        if año_parcial:
            if debug:
                print(f"🔍 Detectado por búsqueda parcial: {año_parcial}")
            return año_parcial
        
        if debug:
            print("❌ No se detectó año")
        return None
    
    def _aplicar_patrones_modelo(self, texto: str, modelo: str, debug: bool = False) -> Optional[int]:
        """
        ✨ APLICA PATRONES APRENDIDOS para detectar años
        """
        patrones = self.patrones_aprendidos.get(modelo, {})
        
        # Intentar cada tipo de patrón
        for tipo_patron, lista_patrones in patrones.items():
            if not lista_patrones:
                continue
                
            año_detectado = self._aplicar_patron_tipo(texto, tipo_patron, debug)
            if año_detectado:
                return año_detectado
        
        return None
    
    def _aplicar_patron_tipo(self, texto: str, tipo_patron: str, debug: bool = False) -> Optional[int]:
        """
        Aplica un tipo específico de patrón
        """
        año_actual = datetime.now().year
        
        if tipo_patron == 'modelo_año_corto':
            # Buscar "modelo XX" donde XX puede ser cualquier año de 2 dígitos
            match = re.search(r'modelo\s+(\d{2})\b', texto, re.IGNORECASE)
            if match:
                año_corto = int(match.group(1))
                año_completo = 2000 + año_corto if año_corto <= 25 else 1900 + año_corto
                if 1980 <= año_completo <= año_actual + 1:
                    if debug:
                        print(f"  🎯 Patrón 'modelo XX': {match.group(1)} → {año_completo}")
                    return año_completo
        
        elif tipo_patron == 'del_año_completo':
            # Buscar "del YYYY"
            match = re.search(r'del\s+(\d{4})\b', texto, re.IGNORECASE)
            if match:
                año = int(match.group(1))
                if 1980 <= año <= año_actual + 1:
                    if debug:
                        print(f"  🎯 Patrón 'del YYYY': {año}")
                    return año
        
        elif tipo_patron == 'año_completo':
            # Buscar "año YYYY"
            match = re.search(r'año\s+(\d{4})\b', texto, re.IGNORECASE)
            if match:
                año = int(match.group(1))
                if 1980 <= año <= año_actual + 1:
                    if debug:
                        print(f"  🎯 Patrón 'año YYYY': {año}")
                    return año
        
        elif tipo_patron == 'año_corto_final':
            # Buscar año de 2 dígitos al final
            match = re.search(r'\b(\d{2})$', texto.strip())
            if match:
                año_corto = int(match.group(1))
                año_completo = 2000 + año_corto if año_corto <= 25 else 1900 + año_corto
                if 1980 <= año_completo <= año_actual + 1:
                    if debug:
                        print(f"  🎯 Patrón 'XX final': {match.group(1)} → {año_completo}")
                    return año_completo
        
        return None
    
    def _normalizar_texto(self, texto: str) -> str:
        """Normaliza texto para búsquedas"""
        texto = texto.lower().strip()
        texto = re.sub(r'[🔥✅💥🚘🔰⚠️🥶]', '', texto)
        texto = re.sub(r'\s+', ' ', texto)
        return texto
    
    def _busqueda_exacta(self, texto_norm: str) -> Optional[int]:
        """Búsqueda exacta en correcciones"""
        return self.correcciones.get(texto_norm)
    
    def _busqueda_parcial(self, texto_norm: str, debug: bool = False) -> Optional[int]:
        """Búsqueda parcial como propuse antes"""
        # Implementación similar a la función anterior...
        return None
    
    def _mostrar_patrones_aprendidos(self):
        """Muestra los patrones que se aprendieron"""
        print(f"\n🧠 PATRONES APRENDIDOS DE {len(self.correcciones)} CORRECCIONES:")
        
        for modelo, patrones in self.patrones_aprendidos.items():
            print(f"\n📱 {modelo.upper()}:")
            for tipo, lista in patrones.items():
                if lista:
                    print(f"  ✅ {tipo}: {len(lista)} ejemplos")
                    for ejemplo in lista[:2]:  # Mostrar 2 ejemplos
                        print(f"    → '{ejemplo.get('formato', 'N/A')}'")
    
    def agregar_correccion(self, texto: str, año: int):
        """Agrega nueva corrección y re-aprende patrones"""
        texto_norm = self._normalizar_texto(texto)
        self.correcciones[texto_norm] = año
        
        # Re-extraer patrones con la nueva corrección
        self._extraer_patrones_automaticamente()
        
        print(f"✅ Corrección agregada y patrones actualizados: '{texto[:30]}...' → {año}")

# EJEMPLO DE USO
def ejemplo_sistema_inteligente():
    """
    Demuestra cómo funcionaría el sistema inteligente
    """
    detector = DetectorAñoInteligente()
    
    # Simular correcciones existentes
    correcciones_ejemplo = {
        "toyota yaris modelo 07": 2007,
        "honda civic modelo 96": 1996, 
        "toyota yaris del 2010": 2010,
        "honda civic año 2015": 2015,
        "suzuki swift 08": 2008
    }
    
    # Cargar correcciones simuladas
    detector.correcciones = correcciones_ejemplo
    detector._extraer_patrones_automaticamente()
    
    # Probar casos nuevos
    casos_prueba = [
        "Toyota yaris modelo 09",  # ✅ Debería detectar 2009 (patrón aprendido)
        "Honda civic modelo 03",   # ✅ Debería detectar 2003 (patrón aprendido)  
        "Toyota yaris del 2012",   # ✅ Debería detectar 2012 (patrón aprendido)
        "Suzuki swift 11",         # ✅ Debería detectar 2011 (patrón aprendido)
        "Toyota corolla modelo 05" # ❓ Modelo nuevo, pero patrón conocido
    ]
    
    print("\n🧪 PRUEBAS DEL SISTEMA INTELIGENTE:")
    print("="*50)
    
    for caso in casos_prueba:
        print(f"\n📱 Caso: '{caso}'")
        resultado = detector.detectar_año_inteligente(caso, debug=True)
        if resultado:
            print(f"  ✅ RESULTADO: {resultado}")
        else:
            print(f"  ❌ No detectado")

if __name__ == "__main__":
    ejemplo_sistema_inteligente()
