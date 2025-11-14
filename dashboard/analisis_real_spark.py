from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil, glob

# ==========================
# Paths absolutos
# ==========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

veh_csv = os.path.join(BASE_DIR, "CalificacionesSpark.csv")
calif_csv = os.path.join(BASE_DIR, "calificacionesv2.csv")

# ==========================
# 1) Sesión Spark
# ==========================
spark = (
    SparkSession.builder
    .appName("AnaliticaREAL_5000")
    .getOrCreate()
)

print("=== Cargando CSV reales ===")
print("Vehículos CSV :", veh_csv)
print("Calif CSV     :", calif_csv)

# ==========================
# 2) Vehículos (catálogo)
# ==========================
df_vehiculos_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(veh_csv)
)

df_vehiculos = (
    df_vehiculos_raw
    .select(
        F.col("id_vehiculo").cast("int").alias("id_vehiculo"),
        F.col("vehiculo").cast("string").alias("vehiculo_label"),
        "anio"
    )
)

# ==========================
# 3) Calificaciones
# ==========================
df_calif_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(calif_csv)
)

df_calif = (
    df_calif_raw
    .withColumnRenamed("carroId", "id_vehiculo")
    .withColumn("id_vehiculo", F.col("id_vehiculo").cast("int"))
    .withColumn("calificacion", F.col("estrellas").cast("double"))
    .where(
        F.col("id_vehiculo").isNotNull() &
        F.col("calificacion").isNotNull()
    )
)

# ==========================
# 4) Conteos debug
# ==========================
n_vehiculos = df_vehiculos.select("id_vehiculo").distinct().count()
n_calif_filas = df_calif.count()
n_calif_vehiculos = df_calif.select("id_vehiculo").distinct().count()

print(f"Vehículos catálogo  : {n_vehiculos}")
print(f"Filas calificaciones : {n_calif_filas}")
print(f"Vehículos con reseña : {n_calif_vehiculos}")

# ==========================
# 5) Agregados por vehículo
# ==========================
df_calif_agg = (
    df_calif
    .groupBy("id_vehiculo")
    .agg(
        F.avg("calificacion").alias("promedio_calificacion"),
        F.count("*").alias("n_resenas")
    )
)

# ==========================
# 6) Join catálogo + calif
# ==========================
df_join = df_vehiculos.join(df_calif_agg, on="id_vehiculo", how="left")

df_final = (
    df_join
    .select(
        "vehiculo_label",
        F.col("promedio_calificacion").alias("modelo"),
        "promedio_calificacion",
        "n_resenas"
    )
    .fillna({
        "promedio_calificacion": 0.0,
        "modelo": 0.0,
        "n_resenas": 0
    })
    .orderBy(F.col("promedio_calificacion").desc())
)

n_final = df_final.count()
print(f"Vehículos agregados (final): {n_final}")

# ==========================
# 7) Escribir CSV final
# ==========================
output_dir_tmp = os.path.join(DATA_DIR, "vehiculos_agg_tmp")
output_csv_final = os.path.join(DATA_DIR, "vehiculos_agg.csv")

if os.path.exists(output_dir_tmp):
    shutil.rmtree(output_dir_tmp)

os.makedirs(DATA_DIR, exist_ok=True)

(
    df_final
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(output_dir_tmp)
)

part_files = glob.glob(os.path.join(output_dir_tmp, "part-*.csv"))
if not part_files:
    raise RuntimeError(f"No se generó ningún part-*.csv en {output_dir_tmp}")

shutil.move(part_files[0], output_csv_final)
shutil.rmtree(output_dir_tmp)

print(f"✅ Archivo generado: {output_csv_final}")
print("Contenido de data/:", os.listdir(DATA_DIR))

spark.stop()
print("✅ Spark job terminado OK — 100% CSV (0% simulado)")
