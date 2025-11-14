from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os, shutil, glob, sys

# --- Spark ---
spark = (SparkSession.builder
    .appName("AnaliticaREAL_Nombres_OK")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress","false")
    .config("spark.ui.bindAddress","0.0.0.0")
    .getOrCreate())

BASE = os.path.expanduser('~/spark-proyecto')

def must_exist(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Falta el archivo requerido: {path}")

def load_csv(path):
    must_exist(path)
    return (spark.read.option("header", True).option("inferSchema", True).csv(path))

def write_one_csv(df, out_path):
    # Soporta archivo/carpeta con mismo nombre
    if os.path.isdir(out_path):
        shutil.rmtree(out_path, ignore_errors=True)
    elif os.path.isfile(out_path):
        os.remove(out_path)
    tmp = out_path + ".tmp"
    (df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp))
    part = glob.glob(os.path.join(tmp, "part-*.csv"))
    if not part:
        raise RuntimeError("No se encontró el CSV generado.")
    shutil.move(part[0], out_path)
    shutil.rmtree(tmp, ignore_errors=True)

def actual_cols_map(df):
    """dict: lower_name -> actual_name (para acceder sin importar may/minúsculas)"""
    return {c.lower(): c for c in df.columns}

# --- Rutas ---
calif_path   = os.path.join(BASE, "calificaciones.csv")
catalog_path = os.path.join(BASE, "catalogo_vehiculos.csv")
out_root     = os.path.join(BASE, "vehiculos_agg.csv")
out_dir      = os.path.join(BASE, "out")
out_dash     = os.path.join(out_dir, "vehiculos_agg.csv")

# --- Cargas ---
calif = load_csv(calif_path)
catalog = load_csv(catalog_path)

# --- Column mapping tolerante a mayúsculas ---
C = actual_cols_map(catalog)
# Debe existir id_vehiculo / marca / modelo / anio (cualquier capitalización)
for req in ["id_vehiculo","marca","modelo","anio"]:
    if req not in C:
        raise ValueError(f"❌ El catálogo debe tener columna '{req}' (cualquier capitalización). Columnas reales: {catalog.columns}")

idveh = C["id_vehiculo"]
marca = C["marca"]
modelo= C["modelo"]
anio  = C["anio"]

# --- Normalizaciones ---
calif = (calif
    .withColumn("carroId_num", F.col("carroId").cast("int"))
    .withColumn("estrellas", F.col("estrellas").cast("double"))
)

catalog = (catalog
    .withColumn("id_vehiculo_num", F.col(idveh).cast("int"))
    .withColumn(marca,  F.trim(F.col(marca)))
    .withColumn(modelo, F.trim(F.col(modelo)))
    .withColumn(anio,   F.col(anio).cast("int"))
)

# --- Intersección de IDs (debe ser > 0) ---
ids_calif   = calif.select("carroId_num").dropna().distinct()
ids_catalog = catalog.select("id_vehiculo_num").dropna().distinct()
inter = ids_calif.join(ids_catalog, "carroId_num" == "id_vehiculo_num", "inner").count()
print(f">>> Intersección IDs calificaciones↔catálogo: {inter} (calif={ids_calif.count()}, catalogo={ids_catalog.count()})")
if inter == 0:
    raise RuntimeError("❌ Los IDs de calificaciones.csv no coinciden con id_vehiculo del catálogo.")

# --- Join por ID ---
joined = calif.join(catalog, calif["carroId_num"] == catalog["id_vehiculo_num"], "inner")

# --- Nombre legible: 'Marca Modelo' (con fallback 'ID ####' si falta info) ---
vehiculo_label = F.concat_ws(" ", F.col(marca), F.col(modelo))
vehiculo_label = F.when(F.length(F.trim(vehiculo_label)) == 0, None).otherwise(F.trim(vehiculo_label))
vehiculo_label = F.coalesce(vehiculo_label, F.concat(F.lit("ID "), F.col("id_vehiculo_num").cast("string")))

# --- Agregado final ---
vehiculos_agg = (joined
    .groupBy(vehiculo_label.alias("vehiculo"), F.col(anio).alias("anio"))
    .agg(
        F.lit("N/A").alias("estado"),
        F.round(F.avg("estrellas"), 2).alias("promedio_calificacion"),
        F.count(F.lit(1)).alias("n_resenas")
    )
    .select("vehiculo","anio","estado","promedio_calificacion","n_resenas")
)

# --- Guardar salidas ---
write_one_csv(vehiculos_agg, out_root)
print(f"✅ Archivo con nombres listo en {out_root}")

os.makedirs(out_dir, exist_ok=True)
write_one_csv(vehiculos_agg, out_dash)
print(f"✅ Copia para dashboard en {out_dash}")

# --- Diagnóstico ---
print(">>> DIAGNÓSTICO")
print(f"vehiculos_agg rows: {vehiculos_agg.count()}")
for r in vehiculos_agg.orderBy(F.desc("n_resenas")).limit(5).collect():
    print(r)

spark.stop()
