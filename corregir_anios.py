import sqlite3
import logging
from datetime import datetime
from utils_analisis import extraer_anio  # solo usamos esta función

logging.basicConfig(level=logging.INFO, format="%(asctime)s ***%(levelname)s*** %(message)s")

def corregir_anios():
    conn = sqlite3.connect("anuncios.db")
    cursor = conn.cursor()

    cursor.execute("SELECT id, texto, anio, link FROM anuncios")
    anuncios = cursor.fetchall()

    for id_, texto, anio_actual, link in anuncios:
        nuevo_anio = extraer_anio(texto)

        logging.info(f"📝 TEXTO CRUDO:\n{texto}")
        logging.info(f"📅 Año detectado: {nuevo_anio}")
        logging.info(f"🔍 {texto.strip().splitlines()[0]} | Año {nuevo_anio} | Relevante: False")  # puedes ajustar esto
        logging.info(f"🔗 {link}")

        if nuevo_anio and nuevo_anio != anio_actual and 1980 <= nuevo_anio <= datetime.now().year:
            cursor.execute("UPDATE anuncios SET anio = ? WHERE id = ?", (nuevo_anio, id_))
            logging.info(f"✅ Anuncio {id_} actualizado: {anio_actual} → {nuevo_anio}\n")

    conn.commit()
    conn.close()
