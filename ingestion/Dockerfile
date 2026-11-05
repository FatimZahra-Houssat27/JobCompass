# Image des producteurs Kafka JobCompass
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
# Les données sont montées en volume dans /data (voir docker-compose.yml)
ENV DATA_DIR=/data PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
CMD ["producer_interactions.py", "--mode", "live"]
