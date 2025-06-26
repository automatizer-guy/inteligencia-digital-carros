import sqlite3
import pandas as pd

# Conéctate a la BD
conn = sqlite3.connect("anuncios.db")

# Lee los primeros 20 registros
df = pd.read_sql_query("SELECT * FROM anuncios LIMIT 20;", conn)

# Cuenta total
total = pd.read_sql_query("SELECT COUNT(*) AS total FROM anuncios;", conn).iloc[0,0]

print(f"Total de anuncios almacenados: {total}\n")
print(df)

conn.close()
