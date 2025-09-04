FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# minimal build deps for cassandra-driver
RUN apt-get update -y && apt-get install -y --no-install-recommends \
      gcc libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000
ENV PORT=5000
CMD ["python","app.py"]
