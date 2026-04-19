FROM python:3.11-slim

WORKDIR /app

# Зависимости системы (для работы websocket и sqlite)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код
COPY *.py ./
COPY config.yaml ./

# Папка для данных
RUN mkdir -p /app/data
ENV DB_PATH=/app/data/alerts.db

# Рестарт при падении (на уровне docker-compose/systemd лучше, но и так сгодится)
CMD ["python", "-u", "main.py"]
