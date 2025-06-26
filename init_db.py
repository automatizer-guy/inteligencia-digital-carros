# init_db.py
import sqlite3

conn = sqlite3.connect("anuncios.db")
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS anuncios (
  link         TEXT    PRIMARY KEY,
  modelo       TEXT,
  anio         INTEGER,
  precio       INTEGER,
  km           TEXT,
  fecha_scrape TEXT,
  roi          REAL,
  score        INTEGER
);
""")

conn.commit()
conn.close()
print("✅ Base de datos inicializada con columna km.")
