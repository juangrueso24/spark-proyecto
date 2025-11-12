# 🚗 Analítica de Reseñas — Demo

Spark + Flask para buscar vehículos por **nombre** (sin IDs) y ver:
- ⭐ *Calificación promedio* (simulada, 2.5–5.0)
- 💬 *Número de reseñas* (simuladas, 5–800)

---

## 📁 Estructura del proyecto

spark-proyecto/
├── analisis_final_spark.py # genera vehiculos_agg.csv (5000 filas simuladas)
├── autos_limpiov8.csv # dataset base de autos
├── dashboard/
│ ├── app.py # servidor Flask
│ └── templates/
│ └── busqueda.html
├── requirements.txt
└── README.md
