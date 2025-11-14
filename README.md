# Proyecto Spark + Dashboard — Reseñas de Vehículos

Este proyecto implementa un sistema completo de análisis de reseñas automotrices utilizando **Apache Spark**, un **dashboard Flask**, y un conjunto de microservicios distribuidos. El sistema procesa más de **5000 vehículos reales**, calificaciones y reseñas para visualizar tendencias, promedios y comportamiento de usuarios.

---

## 🚗 Componentes principales

### **1. Spark Master + Workers**
- Procesamiento distribuido.
- Limpieza, agregación y análisis de datos.
- Genera los CSV finales que consume el dashboard.

### **2. Dashboard Flask (8081)**
Muestra:
- Top vehículos por calificación.
- Top por número de reseñas.
- Distribución de estrellas.
- Actividad de usuarios.
  
**Todo basado en los CSV reales**, NO datos simulados.

### **3. Microservicios**
- Vehículos (catálogo)
- Reseñas
- Calificaciones
- Usuarios

### **4. Base de Datos / Fuentes**
- Dataset Kaggle real (autos_limpiov8.csv)
- Reseñas y calificaciones procesadas en Spark.

---

## ▶️ Cómo ejecutar

### **1. Levantar Spark**

cd ~/spark-3.5.1-bin-hadoop3
./sbin/start-master.sh --host 192.168.100.3 --port 7077 --webui-port 18080
./sbin/start-worker.sh spark://192.168.100.3:7077

Acceso web:
- Master → http://192.168.100.3:18080

### **2. Ejecutar un análisis Spark**

cd ~/spark-proyecto
spark-submit --master spark://192.168.100.3:7077 analisis_final_spark.py

### **3. Ejecutar el Dashboard**

cd ~/spark-proyecto/dashboard
pkill -f app.py || true
python3 app.py

Disponible en:
- http://192.168.100.3:8081/dashboard

---

## 📦 Estructura del proyecto

spark-proyecto/
├── data/
├── dashboard/
├── analisis_spark.py
├── analisis_final_spark.py
├── Dockerfile
├── docker-compose.yml
└── README.md

---

## ✔ Estado final
- Spark funcionando con Master/Workers.
- Dashboard funcionando y mostrando datos reales.
- Repositorio limpio y listo para clonar.
- Total de vehículos mostrado: **5000 (dataset real)**.

