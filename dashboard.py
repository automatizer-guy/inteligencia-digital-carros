import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import os

st.set_page_config(page_title="Análisis de Autos", layout="centered")

st.title("📈 Análisis de Anuncios de Autos")
st.write("Selecciona un modelo y año para analizar su comportamiento en el mercado.")

# 🛣️ Ruta a la base central
DB_PATH = os.path.abspath("upload-artifact/anuncios.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Cargar datos desde la base de datos
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("SELECT * FROM anuncios", conn)
conn.close()

if df.empty:
    st.warning("La base de datos está vacía. Ejecuta el scraper primero.")
    st.stop()

# Convertir fechas a datetime
df["fecha_scrape"] = pd.to_datetime(df["fecha_scrape"])

# Selección de modelo
modelos = sorted(df["modelo"].unique())
modelo_seleccionado = st.selectbox("📌 Filtrar por modelo", modelos)

# Filtrar por modelo
df_modelo = df[df["modelo"] == modelo_seleccionado].copy()

# Filtrado adicional por año
años_disponibles = sorted(df_modelo["anio"].unique())
año_seleccionado = st.selectbox("📅 Año del modelo", años_disponibles)

df_modelo = df_modelo[df_modelo["anio"] == año_seleccionado]

# Calcular métricas
precio_min = df_modelo["precio"].min()
precio_prom = round(df_modelo["precio"].mean(), 1)
roi_prom = round(df_modelo["roi"].mean(), 1)

col1, col2, col3 = st.columns(3)
col1.metric("📉 Precio mínimo", f"Q{precio_min:,}")
col2.metric("📊 Precio promedio", f"Q{precio_prom:,}")
col3.metric("💰 ROI promedio", f"{roi_prom}%")

# Gráfica de precios en el tiempo
st.subheader("📆 Tendencia de Precios")
df_modelo = df_modelo.sort_values("fecha_scrape")
st.line_chart(df_modelo.set_index("fecha_scrape")["precio"])

# Gráfica de ROI
if len(df_modelo) >= 5:
    st.subheader("📊 Tendencia de ROI")
    st.line_chart(df_modelo.set_index("fecha_scrape")["roi"])
else:
    st.info("Se necesitan más datos para mostrar la tendencia de ROI.")

# Histograma de precios
st.subheader("📊 Distribución de Precios por Rangos")
num_bins = 10
precios = df_modelo["precio"].dropna()

if not precios.empty:
    fig, ax = plt.subplots()
    n, bins, patches = ax.hist(precios, bins=num_bins, color="skyblue", edgecolor="black")

    ax.set_xlabel("Rango de precios (Q)")
    ax.set_ylabel("Cantidad de anuncios")
    ax.set_title(f"Distribución de precios para {modelo_seleccionado.title()} {año_seleccionado}")

    st.pyplot(fig)
else:
    st.info("No hay precios disponibles para graficar.")

# Botón para descarga
st.download_button(
    "💾 Descargar datos como CSV",
    df_modelo.to_csv(index=False),
    file_name=f"{modelo_seleccionado}_{año_seleccionado}.csv",
    mime="text/csv"
)
