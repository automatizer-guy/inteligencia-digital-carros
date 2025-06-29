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

# Crear carpeta si no existe
#os.makedirs("screenshots", exist_ok=True)

# Inicializar tabla si no existe
inicializar_tabla_anuncios()

MODELOS_INTERES = [
    "yaris", "civic", "corolla", "sentra", "cr-v", "rav4", "tucson",
    "kia picanto", "chevrolet spark", "honda", "nissan march",
    "suzuki alto", "suzuki swift", "suzuki grand vitara",
    "hyundai accent", "hyundai i10", "kia rio"
]

TIEMPO_CARGA   = 8
SCROLL_PAUSA   = 2
COOKIES_PATH   = "fb_cookies.json"
MIN_RESULTADOS = 20
MAX_RESULTADOS = 30
MINIMO_NUEVOS  = 10
MAX_INTENTOS   = 6

def limpiar_url(link: str) -> str:
    path = urlparse(link).path
    return f"https://www.facebook.com{path}"

async def cargar_contexto_con_cookies(browser):
    print("🔐 Cargando cookies desde GitHub Secret…")
    cookies_json = os.environ.get("FB_COOKIES_JSON", "")
    if not cookies_json:
        print("⚠️ FB_COOKIES_JSON no encontrado. Sesión anónima.")
        return await browser.new_context(locale="es-ES")
    with open("cookies.json", "w", encoding="utf-8") as f:
        f.write(cookies_json)
    context = await browser.new_context(storage_state="cookies.json", locale="es-ES")
    print("✅ Cookies restauradas desde storage_state.")
    return context

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
            #await page.screenshot(path=f"screenshots/{modelo.replace(' ', '_')}.png", full_page=True)
            await asyncio.sleep(TIEMPO_CARGA)

            nuevos_urls = set()
            vistos = set()

            for intento in range(MAX_INTENTOS):
                items = await page.query_selector_all("a[href*='/marketplace/item']")
                print(f"🔄 Intento {intento+1}/{MAX_INTENTOS}: {len(items)} elementos detectados para {modelo}")

                for a in items:
                    texto = (await a.inner_text()).strip()
                    href = await a.get_attribute("href")
                    full_url = limpiar_url(href)
                    if (not texto or full_url in vistos or existe_en_db(full_url) or contiene_negativos(texto)):
                        continue
                    vistos.add(full_url)

                    match = re.search(r"[Qq\$]\s?[\d\.,]+", texto)
                    if not match:
                        pendientes_manual.append(f"🔍 {modelo.title()}\n📝 {texto}\n📎 {full_url}")
                        continue

                    precio = limpiar_precio(match.group())
                    if precio < 3000:
                        continue

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
                    relevante = score >= 6

                    insertar_anuncio_db(
                        url=full_url,
                        modelo=modelo,
                        año=anio,
                        precio=precio,
                        kilometraje=km,
                        roi=roi,
                        score=score,
                        relevante=relevante
                    )

                    resultados.append(
                        f"🚘 *{title}*\n"
                        f"• Año: {anio}\n"
                        f"• Precio: Q{precio:,}\n"
                        f"• Kilometraje: {km}\n"
                        f"• ROI: {roi}%\n"
                        f"• Score: {score}/10\n"
                        f"🔗 {full_url}"
                    )
                    nuevos_urls.add(full_url)

                if len(nuevos_urls) >= MINIMO_NUEVOS:
                    print(f"✅ Se encontraron {len(nuevos_urls)} nuevos para {modelo}")
                    break
                else:
                    await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                    await asyncio.sleep(SCROLL_PAUSA)

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
