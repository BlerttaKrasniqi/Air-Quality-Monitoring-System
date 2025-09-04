FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sensor_simulator.py .
ENV KAFKA_BROKER=kafka:9092
ENV TOPIC=air_quality
CMD ["python","sensor_simulator.py"]
