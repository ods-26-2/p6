FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV KAFKA_BOOTSTRAP_SERVERS="34.66.235.91:9093"

RUN python3 scripts/setup_db.py && python3 scripts/p6_mock_captura.py

CMD ["python3", "-u", "scripts/p6_listener.py"]