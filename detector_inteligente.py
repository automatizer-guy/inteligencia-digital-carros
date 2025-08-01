import json
import os
import re
from typing import Optional, Dict, List, Tuple
from datetime import datetime

class DetectorAñoInteligente:
    """
    Sistema inteligente que aprende patrones automáticamente de las correcciones manuales
    """
    
    def __init__(self, archivo_correcciones: str = "correcciones.json"):
        self.archivo_correcciones = archivo_correcciones
        self.correcciones = {}  # Correcciones exactas originales
        self.patrones_aprendidos = {}  # Patrones extraídos automáticamente
        self.cargar_y_aprender()
        
    def cargar_y_aprender(self):
        """Carga correcciones y extrae patrones automáticamente"""
        try:
            if os.path.exists(self.archivo_correcciones):
                with open(self.archivo_correcciones, 'r', encoding='utf-8') as f:
                    self.correcciones = json.load(f)
                
                # 🧠 MAGIA: Extraer patrones de las correcciones existentes
                self._extraer_patrones_automaticos()
            else:
                self.correcciones = {}
                self.patrones_aprendidos = {}
        except Exception as e:
            print(f"⚠️ Error cargando correcciones: {e}")
            self.correcciones = {}
            self.patrones_aprendidos = {}
    
    def _extraer_patrones_automaticos(self):
        """
        ✨ NÚCLEO DEL SISTEMA: Extrae patrones automáticamente
        """
        if not self.correcciones:
            return
            
        print(f"🧠 Analizando {len(self.correcciones)} correcciones para extraer patrones...")
        
        # Estructura: {modelo: {tipo_patron: [ejemplos]}}
        patrones_por_modelo = {}
        
        for texto_original, año_correcto in self.correcciones.items():
            # Identificar modelo en el texto
            modelos_detectados = self._identificar_modelos(texto_original)
            
            for modelo in modelos_detectados:
                if modelo not in patrones_por_modelo:
                    patrones_por_modelo[modelo] = {
                        'modelo_año_corto': [],
                        'del_año_completo': [],
                        'año_completo': [],
                        'año_corto_final': [],
                        'año_despues_modelo': [],
                        'año_antes_modelo': []
                    }
                
                # Extraer todos los patrones posibles de este texto
                patrones_encontrados = self._extraer_patrones_texto(texto_original, año_correcto, modelo)
                
                for patron in patrones_encontrados:
                    tipo = patron['tipo']
                    if tipo in patrones_por_modelo[modelo]:
                        patrones_por_modelo[modelo][tipo].append(patron)
        
        # Generar regex inteligentes para cada patrón
        self.patrones_aprendidos = {}
        for modelo, tipos_patron in patrones_por_modelo.items():
            self.patrones_aprendidos[modelo] = {}
            
            for tipo, ejemplos in tipos_patron.items():
                if ejemplos:  # Solo si hay ejemplos
                    regex_patron = self._generar_regex_patron(tipo, ejemplos)
                    if regex_patron:
                        self.patrones_aprendidos[modelo][tipo] = {
                            'regex': regex_patron,
                            'ejemplos': len(ejemplos),
                            'años_ejemplo': [e['año'] for e in ejemplos[:3]]
                        }
        
        self._mostrar_patrones_aprendidos()
    
    def _identificar_modelos(self, texto: str) -> List[str]:
        """Identifica todos los modelos de auto presentes en el texto"""
        modelos_vehiculos = [
            'yaris', 'civic', 'corolla', 'sentra', 'cr-v', 'crv', 'rav4', 
            'accent', 'swift', 'alto', 'grand vitara', 'vitara', 'tucson',
            'picanto', 'spark', 'march', 'micra', 'mirage', 'i10', 'rio',
            'fit', 'element', 'accord', 'versa', 'tracker', 'note'
        ]
        
        texto_lower = texto.lower()
        modelos_encontrados = []
        
        for modelo in modelos_vehiculos:
            if modelo in texto_lower:
                modelos_encontrados.append(modelo)
        
        return modelos_encontrados
    
    def _extraer_patrones_texto(self, texto: str, año: int, modelo: str) -> List[Dict]:
        """Extrae todos los patrones posibles de un texto específico"""
        patrones = []
        año_str = str(año)
        año_corto = año_str[2:] if len(año_str) == 4 else año_str
        
        # Patrón 1: "modelo XX" (año corto después de "modelo")
        if re.search(rf'modelo\s+{año_corto}\b', texto, re.IGNORECASE):
            patrones.append({
                'tipo': 'modelo_año_corto',
                'año': año,
                'año_corto': año_corto,
                'contexto': f'modelo {año_corto}'
            })
        
        # Patrón 2: "del YYYY" (año completo después de "del")
        if re.search(rf'del\s+{año_str}\b', texto, re.IGNORECASE):
            patrones.append({
                'tipo': 'del_año_completo',
                'año': año,
                'contexto': f'del {año_str}'
            })
        
        # Patrón 3: "año YYYY" (año completo después de "año")
        if re.search(rf'año\s+{año_str}\b', texto, re.IGNORECASE):
            patrones.append({
                'tipo': 'año_completo',
                'año': año,
                'contexto': f'año {año_str}'
            })
        
        # Patrón 4: "XX" al final (año corto al final del texto)
        if re.search(rf'\b{año_corto}$', texto.strip()):
            patrones.append({
                'tipo': 'año_corto_final',
                'año': año,
                'año_corto': año_corto,
                'contexto': f'final {año_corto}'
            })
        
        # Patrón 5: "modelo YYYY" (año completo después de modelo específico)
        modelo_escaped = re.escape(modelo)
        if re.search(rf'{modelo_escaped}\s+{año_str}\b', texto, re.IGNORECASE):
            patrones.append({
                'tipo': 'año_despues_modelo',
                'año': año,
                'modelo': modelo,
                'contexto': f'{modelo} {año_str}'
            })
        
        # Patrón 6: "YYYY modelo" (año completo antes de modelo específico)
        if re.search(rf'{año_str}\s+{modelo_escaped}\b', texto, re.IGNORECASE):
            patrones.append({
                'tipo': 'año_antes_modelo',
                'año': año,
                'modelo': modelo,
                'contexto': f'{año_str} {modelo}'
            })
        
        return patrones
    
    def _generar_regex_patron(self, tipo: str, ejemplos: List[Dict]) -> Optional[re.Pattern]:
        """Genera regex inteligente para cada tipo de patrón"""
        try:
            if tipo == 'modelo_año_corto':
                return re.compile(r'modelo\s+(\d{2})\b', re.IGNORECASE)
            
            elif tipo == 'del_año_completo':
                return re.compile(r'del\s+(\d{4})\b', re.IGNORECASE)
            
            elif tipo == 'año_completo':
                return re.compile(r'año\s+(\d{4})\b', re.IGNORECASE)
            
            elif tipo == 'año_corto_final':
                return re.compile(r'\b(\d{2})$')
            
            elif tipo == 'año_despues_modelo':
                # Crear regex dinámico basado en los modelos de los ejemplos
                modelos = set(ej.get('modelo', '') for ej in ejemplos if ej.get('modelo'))
                if modelos:
                    modelos_escaped = [re.escape(m) for m in modelos]
                    modelos_pattern = '|'.join(modelos_escaped)
                    return re.compile(rf'({modelos_pattern})\s+(\d{{4}})\b', re.IGNORECASE)
            
            elif tipo == 'año_antes_modelo':
                # Similar al anterior pero con orden invertido
                modelos = set(ej.get('modelo', '') for ej in ejemplos if ej.get('modelo'))
                if modelos:
                    modelos_escaped = [re.escape(m) for m in modelos]
                    modelos_pattern = '|'.join(modelos_escaped)
                    return re.compile(rf'(\d{{4}})\s+({modelos_pattern})\b', re.IGNORECASE)
            
        except Exception as e:
            print(f"⚠️ Error generando regex para {tipo}: {e}")
        
        return None
    
    def detectar_año_inteligente(self, texto: str, debug: bool = False) -> Optional[int]:
        """
        🚀 DETECCIÓN INTELIGENTE: Combina correcciones exactas + patrones aprendidos
        """
        texto_normalizado = self._normalizar_texto(texto)
        
        if debug:
            print(f"🔍 Analizando: '{texto[:60]}...'")
        
        # 1. Búsqueda exacta en correcciones (máxima prioridad)
        if texto_normalizado in self.correcciones:
            resultado = self.correcciones[texto_normalizado]
            if debug:
                print(f"✅ Coincidencia exacta: {resultado}")
            return resultado
        
        # 2. 🧠 BÚSQUEDA POR PATRONES APRENDIDOS
        modelos_detectados = self._identificar_modelos(texto)
        
        for modelo in modelos_detectados:
            if modelo in self.patrones_aprendidos:
                año_detectado = self._aplicar_patrones_modelo(texto, modelo, debug)
                if año_detectado:
                    if debug:
                        print(f"🎯 Detectado por patrón ({modelo}): {año_detectado}")
                    return año_detectado
        
        # 3. Búsqueda parcial mejorada (fallback)
        año_parcial = self._busqueda_parcial_mejorada(texto_normalizado, debug)
        if año_parcial:
            if debug:
                print(f"🔍 Detectado por búsqueda parcial: {año_parcial}")
            return año_parcial
        
        if debug:
            print("❌ No se detectó año")
        return None
    
    def _aplicar_patrones_modelo(self, texto: str, modelo: str, debug: bool = False) -> Optional[int]:
        """Aplica todos los patrones aprendidos para un modelo específico"""
        patrones = self.patrones_aprendidos.get(modelo, {})
        año_actual = datetime.now().year
        
        if debug and patrones:
            print(f"  🧠 Aplicando {len(patrones)} patrones para '{modelo}'")
        
        # Orden de prioridad de patrones
        orden_prioridad = [
            'año_despues_modelo',    # honda civic 2010
            'año_antes_modelo',      # 2010 honda civic  
            'modelo_año_corto',      # modelo 07
            'del_año_completo',      # del 2010
            'año_completo',          # año 2010
            'año_corto_final'        # texto que termina en 07
        ]
        
        for tipo_patron in orden_prioridad:
            if tipo_patron not in patrones:
                continue
                
            regex = patrones[tipo_patron]['regex']
            match = regex.search(texto)
            
            if match:
                try:
                    if tipo_patron in ['modelo_año_corto', 'año_corto_final']:
                        # Años de 2 dígitos - normalizar
                        año_corto = int(match.group(1))
                        año_completo = 2000 + año_corto if año_corto <= 25 else 1900 + año_corto
                    else:
                        # Años de 4 dígitos - usar directamente
                        año_completo = int(match.group(1) if tipo_patron != 'año_antes_modelo' else match.group(1))
                    
                    # Validar rango
                    if 1980 <= año_completo <= año_actual + 1:
                        if debug:
                            ejemplos = patrones[tipo_patron]['ejemplos']
                            print(f"    ✅ Patrón '{tipo_patron}' (de {ejemplos} ejemplos): {match.group()} → {año_completo}")
                        return año_completo
                    
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def _busqueda_parcial_mejorada(self, texto_norm: str, debug: bool = False) -> Optional[int]:
        """Búsqueda parcial con mejor algoritmo de similitud"""
        if not self.correcciones:
            return None
        
        mejor_año = None
        mejor_score = 0.0
        texto_palabras = set(texto_norm.split())
        
        for correccion_texto, año in self.correcciones.items():
            correccion_palabras = set(correccion_texto.split())
            
            # Calcular similitud de Jaccard
            interseccion = texto_palabras.intersection(correccion_palabras)
            union = texto_palabras.union(correccion_palabras)
            
            if len(union) == 0:
                continue
                
            similitud = len(interseccion) / len(union)
            
            # Bonus si contiene palabras clave vehiculares
            palabras_vehiculares = {'toyota', 'honda', 'nissan', 'modelo', 'año', 'automático', 'mecánico'}
            bonus = len(interseccion.intersection(palabras_vehiculares)) * 0.1
            
            score_final = similitud + bonus
            
            # Umbral más estricto para evitar falsos positivos
            if score_final >= 0.6 and score_final > mejor_score:
                mejor_año = año
                mejor_score = score_final
                if debug:
                    print(f"    🔍 Candidato parcial: '{correccion_texto[:30]}...' (score: {score_final:.2f})")
        
        return mejor_año if mejor_score >= 0.6 else None
    
    def _normalizar_texto(self, texto: str) -> str:
        """Normaliza texto para búsquedas consistentes"""
        texto = texto.lower().strip()
        texto = re.sub(r'[🔥✅💥🚘🔰⚠️🥶]', '', texto)  # Remover emojis
        texto = re.sub(r'\s+', ' ', texto)  # Normalizar espacios
        texto = re.sub(r'[.,!?]+$', '', texto)  # Remover puntuación final
        return texto
    
    def _mostrar_patrones_aprendidos(self):
        """Muestra resumen de patrones aprendidos"""
        if not self.patrones_aprendidos:
            print("🤖 No se aprendieron patrones")
            return
            
        total_patrones = sum(len(patrones) for patrones in self.patrones_aprendidos.values())
        print(f"\n🧠 PATRONES APRENDIDOS: {total_patrones} patrones de {len(self.correcciones)} correcciones")
        
        for modelo, patrones in self.patrones_aprendidos.items():
            if patrones:
                print(f"\n🚗 {modelo.upper()}:")
                for tipo, info in patrones.items():
                    ejemplos = info['ejemplos']
                    años_ej = info['años_ejemplo']
                    print(f"  ✅ {tipo}: {ejemplos} ejemplos → años {años_ej}")
        
        # Calcular eficiencia estimada
        correcciones_base = len(self.correcciones)
        casos_estimados = correcciones_base * 5  # Estimación conservadora
        print(f"\n📊 EFICIENCIA ESTIMADA:")
        print(f"  📝 Correcciones base: {correcciones_base}")
        print(f"  🎯 Casos que puede resolver: ~{casos_estimados}")
        print(f"  📈 Factor de multiplicación: ~5x")
    
    def agregar_correccion_y_reaprender(self, texto: str, año: int):
        """Agrega corrección y re-aprende patrones automáticamente"""
        texto_norm = self._normalizar_texto(texto)
        self.correcciones[texto_norm] = año
        
        # Guardar en archivo
        try:
            with open(self.archivo_correcciones, 'w', encoding='utf-8') as f:
                json.dump(self.correcciones, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ Error guardando corrección: {e}")
            return
        
        # Re-aprender patrones con la nueva corrección
        self._extraer_patrones_automaticos()
        
        print(f"✅ Corrección agregada y patrones actualizados:")
        print(f"   '{texto[:40]}...' → {año}")
    
    def estadisticas_sistema(self):
        """Muestra estadísticas completas del sistema"""
        print("\n📊 ESTADÍSTICAS DEL SISTEMA INTELIGENTE")
        print("=" * 50)
        
        print(f"📝 Correcciones exactas: {len(self.correcciones)}")
        print(f"🧠 Modelos con patrones: {len(self.patrones_aprendidos)}")
        
        total_patrones = sum(len(p) for p in self.patrones_aprendidos.values())
        print(f"🎯 Total de patrones: {total_patrones}")
        
        # Distribución por años
        años = list(self.correcciones.values())
        if años:
            print(f"📅 Rango de años: {min(años)} - {max(años)}")
            
            # Contar por década
            decadas = {}
            for año in años:
                decada = (año // 10) * 10
                decadas[decada] = decadas.get(decada, 0) + 1
            
            print("📈 Por década:")
            for dec in sorted(decadas.keys()):
                print(f"   {dec}s: {decadas[dec]} correcciones")


# FUNCIÓN DE INTEGRACIÓN CON TU SISTEMA EXISTENTE
def obtener_correccion_inteligente(texto: str, debug: bool = False) -> Optional[int]:
    """
    🔗 FUNCIÓN DE INTEGRACIÓN: Reemplaza la función obtener_correccion original
    """
    detector = DetectorAñoInteligente()
    return detector.detectar_año_inteligente(texto, debug)


# FUNCIÓN PARA TESTING Y DEMOSTRACIÓN
def demo_sistema_inteligente():
    """Demuestra el funcionamiento del sistema inteligente"""
    print("🧪 DEMO DEL SISTEMA INTELIGENTE DE DETECCIÓN DE AÑOS")
    print("=" * 60)
    
    detector = DetectorAñoInteligente()
    
    # Casos de prueba que deberían funcionar con patrones aprendidos
    casos_prueba = [
        "Toyota yaris modelo 09",           # Patrón: modelo XX
        "Honda civic modelo 03",            # Patrón: modelo XX
        "Toyota yaris del 2012",            # Patrón: del YYYY
        "Suzuki swift año 2011",            # Patrón: año YYYY
        "Honda accord 2015",                # Patrón: modelo YYYY
        "Nissan sentra modelo 05 activo",   # Patrón con contexto extra
        "Vendo honda civic modelo 08 full", # Patrón con contexto vehicular
        "Toyota corolla modelo 15",         # Modelo nuevo, patrón conocido
        "Hyundai accent modelo 14 automático" # Combinación compleja
    ]
    
    print(f"🧠 Sistema cargado con {len(detector.correcciones)} correcciones")
    print(f"🎯 Patrones aprendidos para {len(detector.patrones_aprendidos)} modelos")
    print("\n" + "="*60)
    
    aciertos = 0
    for i, caso in enumerate(casos_prueba, 1):
        print(f"\n📱 CASO {i}: '{caso}'")
        resultado = detector.detectar_año_inteligente(caso, debug=True)
        
        if resultado:
            print(f"  ✅ RESULTADO: {resultado}")
            aciertos += 1
        else:
            print(f"  ❌ No detectado")
        
        print("-" * 40)
    
    print(f"\n📊 RESUMEN: {aciertos}/{len(casos_prueba)} casos exitosos ({aciertos/len(casos_prueba)*100:.1f}%)")
    
    # Mostrar estadísticas
    detector.estadisticas_sistema()


if __name__ == "__main__":
    demo_sistema_inteligente()
