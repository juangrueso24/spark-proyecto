from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil, glob

# ----------------------------
# 1) Crear sesión de Spark
# ----------------------------
spark = (
    SparkSession.builder
    .appName("AnaliticaREAL_5000")
    .getOrCreate()
)

print("=== Cargando CSV reales ===")

# ----------------------------
# 2) Leer CSV de vehículos
# ----------------------------
df_vehiculos_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("CalificacionesSpark.csv")
)
print("✅ CSV de vehículos leído")

df_vehiculos = (
    df_vehiculos_raw
    .select(
        F.col("id_vehiculo").cast("int").alias("id_vehiculo"),
        F.col("vehiculo").cast("string").alias("vehiculo_label"),
        "anio"
    )
)

# ----------------------------
# 3) Leer CSV de calificaciones
# ----------------------------
df_calif_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("calificacionesv2.csv")
)
print("✅ CSV de calificaciones leído")

df_calif = (
    df_calif_raw
    .withColumnRenamed("carroId", "id_vehiculo")
    .withColumn("id_vehiculo", F.col("id_vehiculo").cast("int"))
    .withColumn("calificacion", F.col("estrellas").cast("double"))
)

df_calif = df_calif.where(
    F.col("id_vehiculo").isNotNull() & F.col("calificacion").isNotNull()
)

# ----------------------------
# 4) Debug de conteos
# ----------------------------
n_vehiculos = df_vehiculos.count()
n_calif = df_calif.count()

print(f"Vehículos catálogo  : {n_vehiculos}")
print(f"Filas calificaciones : {n_calif}")

# ----------------------------
# 5) Agregar calificaciones
# ----------------------------
df_calif_agg = (
    df_calif.groupBy("id_vehiculo")
    .agg(
        F.avg("calificacion").alias("promedio_calificacion"),
        F.count("*").alias("n_resenas")
    )
)

# ----------------------------
# 6) LEFT JOIN para mostrar los 5000 autos
# ----------------------------
df_join = df_vehiculos.join(df_calif_agg, on="id_vehiculo", how="left")

df_final = (
    df_join
    .select(
        "vehiculo_label",
        F.col("promedio_calificacion").alias("modelo"),
        "promedio_calificacion",
        "n_resenas"
    )
    .fillna({"promedio_calificacion": 0.0, "modelo": 0.0, "n_resenas": 0})
    .orderBy(F.col("promedio_calificacion").desc())
)

print("Vehículos agregados (final):", df_final.count())

# ----------------------------
# 7) Guardar resultado
# ----------------------------
output_dir_tmp = "data/vehiculos_agg_tmp"
output_csv = "data/vehiculos_agg.csv"

os.makedirs("data", exist_ok=True)
if os.path.exists(output_dir_tmp):
    shutil.rmtree(output_dir_tmp)

df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir_tmp)

# mover part-*.csv a vehiculos_agg.csv
part = glob.glob(output_dir_tmp + "/part-*.csv")[0]
shutil.move(part, output_csv)
shutil.rmtree(output_dir_tmp)

print("✅ Archivo generado:", output_csv)

spark.stop()
print("✅ Spark job terminado OK — 100% CSV (0% simulado)")
