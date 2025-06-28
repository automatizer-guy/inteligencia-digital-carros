import sqlite3
import os

# 📍 Ruta alineada con el sistema automatizado
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS anuncios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  link TEXT UNIQUE,
  modelo TEXT,
  anio INTEGER,
  precio INTEGER,
  km TEXT,
  fecha_scrape TEXT,
  roi REAL,
  score INTEGER
);
""")
conn.commit()
conn.close()
print(f"✅ Base de datos inicializada correctamente en: {DB_PATH}")
