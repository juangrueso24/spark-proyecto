from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil, glob

# ========= Spark =========
spark = (SparkSession.builder
    .appName("AnaliticaREAL_Nombres_OK_DEF")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress","false")
    .config("spark.ui.bindAddress","0.0.0.0")
    .getOrCreate())

BASE = os.path.expanduser('~/spark-proyecto')
CALIF = os.path.join(BASE, "calificaciones.csv")
CATAL = os.path.join(BASE, "catalogo_vehiculos.csv")
OUT_ROOT = os.path.join(BASE, "vehiculos_agg.csv")      # copia “raíz” (por si quieres abrir rápido)
OUT_DIR  = os.path.join(BASE, "out")
OUT_DASH = os.path.join(OUT_DIR, "vehiculos_agg.csv")    # el que lee el dashboard

def must(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Falta {path}")

def load_csv(path):
    must(path)
    return (spark.read.option("header", True).option("inferSchema", True).csv(path))

def write_one_csv(df, out_path):
    # borra archivo o carpeta homónima
    if os.path.isdir(out_path): shutil.rmtree(out_path, ignore_errors=True)
    elif os.path.isfile(out_path): os.remove(out_path)
    tmp = out_path + ".tmp"
    (df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp))
    part = glob.glob(os.path.join(tmp, "part-*.csv"))
    if not part:
        raise RuntimeError("No se generó el CSV (part-*.csv).")
    shutil.move(part[0], out_path)
    shutil.rmtree(tmp, ignore_errors=True)

def colmap(df):
    # acceso tolerante a mayúsculas: lower->col real
    return {c.lower(): c for c in df.columns}

# ====== Cargas ======
calif = load_csv(CALIF)
catalog = load_csv(CATAL)

# ====== Normalizaciones mínimas ======
# calificaciones: carroId / estrellas
Cc = colmap(calif)
if "carroid" not in Cc:  # por si la primera columna no vino con nombre
    first = calif.columns[0]
    calif = calif.withColumnRenamed(first, "carroId")
    Cc = colmap(calif)

calif = (calif
    .withColumn("carroId_num", F.col(Cc["carroid"]).cast("int"))
    .withColumn("estrellas", F.col(Cc.get("estrellas", Cc["carroid"])).cast("double"))  # intentamos 'estrellas'
)

# catálogo: id_vehiculo / marca / modelo / anio (tolerante a capitalización)
K = colmap(catalog)
for req in ["id_vehiculo", "marca", "modelo", "anio"]:
    if req not in K:
        raise ValueError(f"❌ '{req}' no está en catalogo_vehiculos.csv. Columnas: {catalog.columns}")

catalog = (catalog
    .withColumn("id_vehiculo_num", F.col(K["id_vehiculo"]).cast("int"))
    .withColumn(K["marca"],  F.trim(F.col(K["marca"])))
    .withColumn(K["modelo"], F.trim(F.col(K["modelo"])))
    .withColumn(K["anio"],   F.col(K["anio"]).cast("int"))
)

# ====== Join por ID (la condición debe ser Column, no strings) ======
joined = calif.join(
    catalog,
    calif["carroId_num"] == catalog["id_vehiculo_num"],
    "inner"
)

# Diagnóstico rápido de intersección
ids_inter = joined.select("carroId_num").distinct().count()
print(f">>> Intersección IDs calificaciones↔catálogo: {ids_inter}")

if ids_inter == 0:
    raise RuntimeError("❌ No hay intersección de IDs entre calificaciones y catálogo.")

# ====== Etiqueta legible: 'Marca Modelo' con fallback 'ID ####' solo si falta nombre ======
vehiculo_label = F.concat_ws(" ", F.col(K["marca"]), F.col(K["modelo"]))
vehiculo_label = F.when(F.length(F.trim(vehiculo_label)) == 0, None).otherwise(F.trim(vehiculo_label))
vehiculo_label = F.coalesce(vehiculo_label, F.concat(F.lit("ID "), F.col("id_vehiculo_num").cast("string")))

# ====== Agregado final (lo que consume el dashboard) ======
vehiculos_agg = (joined
    .groupBy(vehiculo_label.alias("vehiculo"), F.col(K["anio"]).alias("anio"))
    .agg(
        F.lit("N/A").alias("estado"),
        F.round(F.avg("estrellas"), 2).alias("promedio_calificacion"),
        F.count(F.lit(1)).alias("n_resenas")
    )
    .select("vehiculo","anio","estado","promedio_calificacion","n_resenas")
)

# ====== Guardar ======
write_one_csv(vehiculos_agg, OUT_ROOT)
print(f"✅ Archivo para ver rápido: {OUT_ROOT}")

os.makedirs(OUT_DIR, exist_ok=True)
write_one_csv(vehiculos_agg, OUT_DASH)
print(f"✅ Archivo para dashboard: {OUT_DASH}")

# ====== Diagnóstico ======
total = vehiculos_agg.count()
print(">>> DIAGNÓSTICO")
print(f"Filas finales: {total}")
for r in vehiculos_agg.orderBy(F.desc("n_resenas")).limit(5).collect():
    print(r)

spark.stop()
