import sqlite3
import pandas as pd
import os

# 🛣️ Ruta a la base unificada
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Conéctate a la BD
conn = sqlite3.connect(DB_PATH)

# Lee los primeros 20 registros
df = pd.read_sql_query("SELECT * FROM anuncios LIMIT 20;", conn)

# Cuenta total
total = pd.read_sql_query("SELECT COUNT(*) AS total FROM anuncios;", conn).iloc[0, 0]

print(f"Total de anuncios almacenados: {total}\n")
print(df)

conn.close()
