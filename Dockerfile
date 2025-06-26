
FROM mcr.microsoft.com/playwright/python:v1.35.0-focal
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN playwright install --with-deps
CMD ["python", "bot_telegram_marketplace.py"]
