# 🚗 Analítica de Reseñas — Demo

Spark + Flask para buscar vehículos por **nombre** (sin IDs) y ver:
- ⭐ Calificación promedio (simulada)
- 💬 Número de reseñas (simuladas)

## Requisitos
- Python 3.10+ (ideal 3.11)

## Instalación rápida
```bash
# clonar
git clone https://github.com/juangrueso24/spark-proyecto.git
cd spark-proyecto

# (opcional) entorno virtual
python3 -m venv venv && source venv/bin/activate

# deps
pip install -r requirements.txt

# generar datos simulados (5000 vehículos, SIN NaN)
python3 analisis_final_spark.py

# lanzar dashboard
cd dashboard
python3 app.py

