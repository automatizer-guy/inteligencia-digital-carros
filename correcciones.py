import json
import os

CORRECCIONES_FILE = "correcciones.json"

def cargar_correcciones():
    if not os.path.exists(CORRECCIONES_FILE):
        return {}
    with open(CORRECCIONES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_correccion(texto, año):
    correcciones = cargar_correcciones()
    correcciones[texto.strip().lower()] = año
    with open(CORRECCIONES_FILE, "w", encoding="utf-8") as f:
        json.dump(correcciones, f, indent=2, ensure_ascii=False)

def obtener_correccion(texto):
    correcciones = cargar_correcciones()
    return correcciones.get(texto.strip().lower())
