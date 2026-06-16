FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer ./producer
COPY src ./src

CMD ["python", "producer/producer_kafka.py"]
