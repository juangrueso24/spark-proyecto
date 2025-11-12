import os, re, unicodedata, glob, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, length, trim, to_date, date_format, round, coalesce
)

# ---------- utilidades ----------
def normalize_name(name: str) -> str:
    nfkd = unicodedata.normalize('NFKD', name)
    s = "".join([c for c in nfkd if not unicodedata.combining(c)])
    s = re.sub(r'(?<!^)([A-Z])', r'_\1', s)
    s = re.sub(r'[^0-9a-zA-Z_]+', '_', s).strip('_').lower()
    return s

def normalize_df_cols(df):
    out = df
    for c in df.columns:
        nc = normalize_name(c)
        if nc != c:
            out = out.withColumnRenamed(c, nc)
    return out

def pick(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def write_single_csv(df, out_csv_path, tmp_dir):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    part = sorted(glob.glob(os.path.join(tmp_dir, "part-*.csv")))[0]
    shutil.copyfile(part, out_csv_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)

BASE = os.path.expanduser('~/spark-proyecto')
spark = (SparkSession.builder.appName("Analisis_PaginaWeb").master("local[*]").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def read_csv(path):
    if not os.path.exists(path): return None
    return (spark.read.option("header", True).option("inferSchema", True).csv(path))

VISTA = os.environ.get("VISTA","vehiculos").strip().lower()

# ======== VISTA: CALIFICACIONES ========
if VISTA == "calificaciones":
    calif = read_csv(os.path.join(BASE,'calificaciones.csv'))
    if calif is None: raise SystemExit("Falta calificaciones.csv")
    calif = normalize_df_cols(calif)

    # Mapear columnas flexibles
    id_col  = pick(calif, ["id_vehiculo","carro_id","vehiculo_id","idvehiculo","carroid"])
    ratecol = pick(calif, ["calificacion","estrellas","rating","score"])
    fcol    = pick(calif, ["creado_en","fecha","created_at","datetime","ts"])
    if ratecol is None:
        raise SystemExit(f"No se encontró columna de calificación. Columnas: {calif.columns}")

    if id_col:   calif = calif.withColumn("id_vehiculo", trim(col(id_col)))
    calif = calif.withColumn("calificacion", col(ratecol).cast("double"))
    if fcol:     calif = calif.withColumn("fecha", to_date(col(fcol)))

    # Distribución de estrellas
    dist = (calif.groupBy("calificacion").agg(count("*").alias("n")).orderBy("calificacion"))
    write_single_csv(dist, os.path.join(BASE,"distribucion_estrellas.csv"), os.path.join(BASE,"out_tmp_dist"))

    # Serie mensual si hay fecha
    if fcol:
        mensual = (calif.withColumn("ym", date_format(col("fecha"),"yyyy-MM"))
                        .groupBy("ym").agg(count("*").alias("n_calif")).orderBy("ym"))
        write_single_csv(mensual, os.path.join(BASE,"calificaciones_mensual.csv"), os.path.join(BASE,"out_tmp_calif_mensual"))

    print("✅ VISTA=calificaciones -> distribucion_estrellas.csv (+ calificaciones_mensual.csv si había fecha)")

# ======== VISTA: RESENAS ========
elif VISTA == "resenas":
    res = read_csv(os.path.join(BASE,'resenas.csv'))
    if res is None: raise SystemExit("Falta resenas.csv")
    res = normalize_df_cols(res)

    # Mapear columnas flexibles
    usercol = pick(res, ["usuario","username","user","author"])
    id_col  = pick(res, ["id_vehiculo","carro_id","vehiculo_id","idvehiculo","carroid"])
    txtcol  = pick(res, ["resena","comentario","texto","review","coment"])
    fcol    = pick(res, ["fecha","creado_en","created_at","datetime","ts"])
    if fcol is None:
        raise SystemExit(f"No se encontró columna de fecha en resenas.csv. Columnas: {res.columns}")

    if id_col: res = res.withColumn("id_vehiculo", trim(col(id_col)))
    res = res.withColumn("fecha_d", to_date(col(fcol)))

    # Serie diaria de reseñas
    diarias = res.groupBy("fecha_d").agg(count("*").alias("n_resenas")).orderBy("fecha_d")
    write_single_csv(diarias, os.path.join(BASE,"resenas_diarias.csv"), os.path.join(BASE,"out_tmp_res_diarias"))

    # Usuarios (si hay user y texto)
    if usercol:
        if txtcol is None:
            # si no hay texto, igual contar por usuario
            usuarios = res.groupBy(usercol).agg(count("*").alias("n_resenas")).orderBy(col("n_resenas").desc())
        else:
            usuarios = (res.groupBy(usercol)
                           .agg(count("*").alias("n_resenas"),
                                avg(length(col(txtcol))).alias("longitud_media_resena"))
                           .orderBy(col("n_resenas").desc()))
        usuarios = usuarios.withColumnRenamed(usercol, "usuario")
        write_single_csv(usuarios, os.path.join(BASE,"usuarios_agg.csv"), os.path.join(BASE,"out_tmp_usuarios"))

    print("✅ VISTA=resenas -> resenas_diarias.csv (+ usuarios_agg.csv si user)")

# ======== VISTA: VEHICULOS (rating por id) ========
else:
    calif = read_csv(os.path.join(BASE,'calificaciones.csv'))
    res   = read_csv(os.path.join(BASE,'resenas.csv'))
    if calif is None or res is None:
        raise SystemExit("Faltan calificaciones.csv y/o resenas.csv")

    calif = normalize_df_cols(calif)
    res   = normalize_df_cols(res)

    idc  = pick(calif, ["id_vehiculo","carro_id","vehiculo_id","idvehiculo","carroid"])
    rate = pick(calif, ["calificacion","estrellas","rating","score"])
    if idc is None or rate is None:
        raise SystemExit(f"En calificaciones faltan columnas id y/o rating. Cols: {calif.columns}")
    calif = calif.withColumn("id_vehiculo", trim(col(idc))).withColumn("calificacion", col(rate).cast("double"))

    idr  = pick(res, ["id_vehiculo","carro_id","vehiculo_id","idvehiculo","carroid"])
    txt  = pick(res, ["resena","comentario","texto","review","coment"])
    if idr:
        res = res.withColumn("id_vehiculo", trim(col(idr)))
    # Agregados
    calif_agg = (calif.groupBy("id_vehiculo")
                      .agg(avg(col("calificacion")).alias("promedio_calificacion"),
                           count("*").alias("n_calificaciones")))
    if idr:
        if txt:
            res_agg = (res.groupBy("id_vehiculo")
                         .agg(count("*").alias("n_resenas"),
                              round(avg(length(coalesce(col(txt), col(txt)))),1).alias("longitud_media_resena")))
        else:
            res_agg = (res.groupBy("id_vehiculo")
                         .agg(count("*").alias("n_resenas")))
        veh = calif_agg.join(res_agg, "id_vehiculo", "outer")\
               .fillna({"n_resenas":0,"longitud_media_resena":0})
    else:
        veh = calif_agg.withColumn("n_resenas", col("n_calificaciones")*0)\
               .withColumn("longitud_media_resena", col("n_calificaciones")*0)

    veh = veh.orderBy(col("promedio_calificacion").desc_nulls_last())
    write_single_csv(veh, os.path.join(BASE,"vehiculos_agg.csv"), os.path.join(BASE,"out_tmp_veh"))

    print("✅ VISTA=vehiculos -> vehiculos_agg.csv")

spark.stop()
