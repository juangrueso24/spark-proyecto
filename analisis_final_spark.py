from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, rand, floor
import os

spark = (SparkSession.builder
    .appName("AnaliticaFinal")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

BASE = os.path.expanduser('~/spark-proyecto')

# ==== CARGA BASE ====
autos = spark.read.option("header", True).option("inferSchema", True).csv(f"{BASE}/autos_limpiov8.csv")

# ==== SUBMUESTREO ====
# Selecciona 5000 vehículos aleatorios para no sobrecargar la vista
autos = autos.sample(fraction=0.05, seed=42).limit(5000)

# ==== SIMULACIÓN DE MÉTRICAS ====
# Promedios entre 2.5 y 5.0 (más variado)
# Número de reseñas entre 5 y 800 (también variado)
autos = (autos
    .withColumn("promedio_calificacion", round(2.5 + rand() * 2.5, 2))
    .withColumn("n_resenas", (floor(5 + rand() * 795)).cast("int"))
)

# ==== SELECCIÓN FINAL ====
final = autos.select(
    col("Model").alias("vehiculo"),
    col("Year").alias("anio"),
    col("Status").alias("estado"),
    "promedio_calificacion",
    "n_resenas"
).orderBy(col("promedio_calificacion").desc())

# ==== EXPORTACIÓN ====
out = f"{BASE}/vehiculos_agg.csv"
final.toPandas().to_csv(out, index=False)

print(f"✅ Vehículos simulados (5000 filas) exportados en {out}")
spark.stop()
