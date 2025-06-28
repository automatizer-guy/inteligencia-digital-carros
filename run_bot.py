import asyncio
from bot_telegram_marketplace import enviar_ofertas

if __name__ == "__main__":
    print("🤖 Ejecutando bot de Telegram...")
    asyncio.run(enviar_ofertas())
