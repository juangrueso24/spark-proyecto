from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil, glob, sys

spark = (SparkSession.builder
    .appName("AnaliticaREAL_Nombres_Def")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress","false")
    .config("spark.ui.bindAddress","0.0.0.0")
    .getOrCreate())

BASE = os.path.expanduser('~/spark-proyecto')

def load_csv(name):
    p = os.path.join(BASE, name)
    if not os.path.exists(p):
        raise FileNotFoundError(f"Falta {name} en {BASE}")
    return spark.read.option("header", True).option("inferSchema", True).csv(p)

def write_one_csv(df, out_path):
    if os.path.isdir(out_path): shutil.rmtree(out_path, ignore_errors=True)
    elif os.path.isfile(out_path): os.remove(out_path)
    tmp = out_path + ".tmp"
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(tmp))
    part = glob.glob(os.path.join(tmp, "part-*.csv"))
    if not part:
        raise RuntimeError("No se encontró el CSV generado.")
    shutil.move(part[0], out_path)
    shutil.rmtree(tmp, ignore_errors=True)

# ========= CARGAS =========
calif = load_csv("calificaciones.csv")          # columnas esperadas: carroId, estrellas, ...
catalog = load_csv("catalogo_vehiculos.csv")    # columnas: id_vehiculo, marca, modelo, anio

# Normalizaciones
calif = (calif
    .withColumn("carroId_num", F.col("carroId").cast("int"))
    .withColumn("estrellas", F.col("estrellas").cast("double"))
)

catalog = (catalog
    .withColumn("id_vehiculo_num", F.col("id_vehiculo").cast("int"))
    .withColumn("marca", F.trim(F.col("marca")))
    .withColumn("modelo", F.trim(F.col("modelo")))
    .withColumn("anio", F.col("anio").cast("int"))
)

# ========= JOIN POR ID (carroId → id_vehiculo) =========
joined = (calif
    .join(catalog, on=(F.col("carroId_num") == F.col("id_vehiculo_num")), how="inner")
)

# Vehiculo legible: "Marca Modelo"  (si falta algo, cae a lo disponible)
vehiculo_label = F.when(
    (F.col("marca").isNotNull()) & (F.col("modelo").isNotNull()),
    F.concat_ws(" ", F.col("marca"), F.col("modelo"))
).otherwise(
    F.coalesce(F.col("modelo"), F.col("marca"), F.lit("Desconocido"))
)

# ========= AGREGADO =========
vehiculos_agg = (joined
    .groupBy(vehiculo_label.alias("vehiculo"), F.col("anio"))
    .agg(
        F.lit("N/A").alias("estado"),
        F.round(F.avg("estrellas"), 2).alias("promedio_calificacion"),
        F.count(F.lit(1)).alias("n_resenas")
    )
    .select("vehiculo","anio","estado","promedio_calificacion","n_resenas")
)

# ========= EXPORTA PARA EL DASHBOARD =========
out1 = os.path.join(BASE, "vehiculos_agg.csv")
write_one_csv(vehiculos_agg, out1)
print(f"✅ Archivo con nombres listo en {out1}")

out_dir = os.path.join(BASE, "out"); os.makedirs(out_dir, exist_ok=True)
out2 = os.path.join(out_dir, "vehiculos_agg.csv")
write_one_csv(vehiculos_agg, out2)
print(f"✅ Copia para dashboard en {out2}")

print(">>> DIAGNÓSTICO")
print(f"vehiculos_agg rows: {vehiculos_agg.count()}")
for r in vehiculos_agg.orderBy(F.desc("n_resenas")).limit(5).collect():
    print(r)

spark.stop()
