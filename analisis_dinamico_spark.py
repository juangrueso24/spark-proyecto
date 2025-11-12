from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, lower, trim
import sys, os

spark = (SparkSession.builder
    .appName("AnaliticaDinamica")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

BASE = os.path.expanduser('~/spark-proyecto')
TIPO = sys.argv[1] if len(sys.argv) > 1 else 'calificaciones'

def load_csv(name):
    path = os.path.join(BASE, name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe {path}")
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

# ==== CARGA DE ARCHIVOS ====
autos = load_csv("autos_limpiov8.csv")
# Normaliza columnas de texto
autos = autos.withColumn("modelo_norm", lower(trim(col("Model"))))

if TIPO == "calificaciones":
    df = load_csv("calificaciones.csv")
    # No hay marca, solo carroId. Simulamos un mapeo textual (por modelo)
    # Supongamos que carroId está vinculado a un nombre del modelo textual, lo renombramos:
    # Este bloque puede adaptarse si luego agregas una columna 'modelo' en calificaciones.csv
    df = df.withColumnRenamed("carroId", "modelo_raw")
    df = df.withColumn("modelo_norm", lower(trim(col("modelo_raw"))))

    joined = df.join(autos, on="modelo_norm", how="left")

    result = (joined.groupBy("Model")
        .agg(avg("estrellas").alias("promedio_calificacion"),
             count("*").alias("total_calificaciones"))
        .orderBy(desc("promedio_calificacion"))
    )

    print("== Mejores calificados ==")
    result.show(10, truncate=False)
    out = os.path.join(BASE, "salida_mejores_calificados.csv")

elif TIPO == "resenas":
    df = load_csv("resenas.csv")
    # Asumimos que también tiene un nombre o ID de modelo
    for c in df.columns:
        if c.lower() in ["id_vehiculo", "carroid", "modelo"]:
            df = df.withColumnRenamed(c, "modelo_raw")
    df = df.withColumn("modelo_norm", lower(trim(col("modelo_raw"))))
    joined = df.join(autos, on="modelo_norm", how="left")
    result = (joined.groupBy("Model")
        .agg(count("*").alias("n_resenas"))
        .orderBy(desc("n_resenas"))
    )
    print("== Vehículos con más reseñas ==")
    result.show(10, truncate=False)
    out = os.path.join(BASE, "salida_resenas.csv")

elif TIPO == "autos":
    result = autos.select("Model", "Year", "Status", "Price", "MSRP")
    print("== Catálogo de autos ==")
    result.show(10, truncate=False)
    out = os.path.join(BASE, "salida_autos.csv")

elif TIPO == "usuarios":
    df = load_csv("usuarios.csv")
    print("== Usuarios ==")
    print(f"Total: {df.count()}")
    df.describe().show()
    result = df
    out = os.path.join(BASE, "salida_usuarios.csv")

else:
    raise ValueError("Tipo no reconocido")

(result.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(out))

print(f"\n✅ Resultado exportado en {out}")
spark.stop()
