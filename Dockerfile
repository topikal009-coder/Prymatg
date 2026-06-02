FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir pyrogram tgcrypto aiohttp

COPY . .

RUN mkdir -p /app/data /app/data/sessions /app/data/user_settings /app/bot_session

CMD ["python", "main.py"]
