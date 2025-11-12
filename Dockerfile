# Base Python + Java para PySpark
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

# Java 21 para PySpark 3.5.x
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Requisitos primero (mejor caché)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar el proyecto
COPY . /app

# Puerto Flask
EXPOSE 8081

# Al arrancar: generar CSV y levantar Flask
CMD bash -lc "python3 analisis_final_spark.py && python3 dashboard/app.py"
