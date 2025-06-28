import re
import os
import json
import asyncio
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from utils_analisis import (
    limpiar_precio, contiene_negativos, puntuar_anuncio,
    calcular_roi, coincide_modelo,
    existe_en_db, insertar_anuncio_db, inicializar_tabla_anuncios
)

# Inicializar tabla si no existe
inicializar_tabla_anuncios()

MODELOS_INTERES = [
    "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
    "kia picanto", "chevrolet spark", "honda", "nissan march",
    "suzuki alto", "suzuki swift", "suzuki grand vitara",
    "hyundai accent", "hyundai i10", "kia rio"
]

TIEMPO_CARGA   = 8
SCROLL_VECES   = 4
SCROLL_PAUSA   = 2
COOKIES_PATH   = "fb_cookies.json"
MIN_RESULTADOS = 20
MAX_RESULTADOS = 30

def limpiar_url(link: str) -> str:
    path = urlparse(link).path
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser):
    ctx = await browser.new_context(locale="es-ES")
    if os.path.exists(COOKIES_PATH):
        with open(COOKIES_PATH, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        await ctx.add_cookies(cookies)
        print("✅ Cookies cargadas.")
    else:
        print("⚠️ No hay cookies. Sesión anónima.")
    return ctx

async def buscar_autos_marketplace():
    print("\n🔎 Iniciando búsqueda en Marketplace…")
    resultados = []
    pendientes_manual = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await cargar_contexto_con_cookies(browser)
        page = await ctx.new_page()

        for modelo in MODELOS_INTERES:
            if len(resultados) >= MIN_RESULTADOS:
                break

            print(f"\n🔍 {modelo.upper()}…")
            url = (
                "https://www.facebook.com/marketplace/guatemala/search/"
                f"?query={modelo.replace(' ','%20')}"
                "&minPrice=1000&maxPrice=60000"
                "&sortBy=best_match&conditions=used_good_condition"
            )
            await page.goto(url)
            await asyncio.sleep(TIEMPO_CARGA)
            for _ in range(SCROLL_VECES):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(SCROLL_PAUSA)

            items = await page.query_selector_all("a[href*='/marketplace/item']")
            print(f"💾 {len(items)} anuncios encontrados para {modelo}")

            vistos = set()
            for a in items:
                texto = (await a.inner_text()).strip()
                href = await a.get_attribute("href")
                full_url = limpiar_url(href)

                if (not texto
                    or full_url in vistos
                    or existe_en_db(full_url)
                    or contiene_negativos(texto)):
                    continue

                match = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
                if not match:
                    pendientes_manual.append(f"🔍 {modelo.title()}\n📝 {texto}\n📎 {full_url}")
                    continue

                precio = limpiar_precio(match.group())
                if precio < 3000:
                    continue  # precio sospechoso, se descarta

                if not coincide_modelo(texto, modelo):
                    continue

                lines = [l for l in texto.splitlines() if l]
                title = modelo.title()
                anio = None

                try:
                    if len(lines) > 1 and lines[1].split()[0].isdigit():
                        posible_anio = int(lines[1].split()[0])
                        if 1990 <= posible_anio <= 2030:
                            anio = posible_anio
                            title = " ".join(lines[1].split()[1:]).title()
                except:
                    pass

                if not anio:
                    match_anio = re.search(r"\b(19[9]\d|20[0-2]\d|2030)\b", texto)
                    if match_anio:
                        anio = int(match_anio.group())

                if not anio or precio == 0:
                    continue

                km = lines[3] if len(lines) > 3 else ""
                roi = calcular_roi(modelo, precio, anio)
                score = puntuar_anuncio(title, precio, texto)

                insertar_anuncio_db(full_url, modelo, anio, precio, km, roi, score)

                resultados.append(
                    f"🚘 *{title}*\n"
                    f"• Año: {anio}\n"
                    f"• Precio: Q{precio:,}\n"
                    f"• Kilometraje: {km}\n"
                    f"• ROI: {roi}%\n"
                    f"• Score: {score}/10\n"
                    f"🔗 {full_url}"
                )
                vistos.add(full_url)

                if len(resultados) >= MAX_RESULTADOS:
                    break

        await browser.close()

    return resultados, pendientes_manual
if __name__ == "__main__":
    async def main():
        brutos, pendientes = await buscar_autos_marketplace()
        for msg in brutos:
            print(msg + "\n")
        if pendientes:
            print("📌 Pendientes de revisión manual:")
            for p in pendientes:
                print(p + "\n")
    asyncio.run(main())
