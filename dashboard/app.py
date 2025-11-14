import os
import pandas as pd
from flask import Flask, render_template, request

app = Flask(__name__)

# ===========================
# RUTAS DE ARCHIVOS
# ===========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))      # .../spark-proyecto/dashboard
ROOT_DIR = os.path.dirname(BASE_DIR)                       # .../spark-proyecto

CSV_VEHICULOS = os.path.join(ROOT_DIR, "data", "vehiculos_agg.csv")
CSV_CALIF = os.path.join(ROOT_DIR, "calificacionesv2.csv")
CSV_RESENAS = os.path.join(ROOT_DIR, "resenas.csv")  # ajusta si lo tienes en /data


# ===========================
# HELPERS
# ===========================
def cargar_vehiculos():
    if not os.path.exists(CSV_VEHICULOS):
        raise RuntimeError(f"No encuentro el CSV de vehículos en: {CSV_VEHICULOS}")

    df = pd.read_csv(CSV_VEHICULOS)

    # Normalizar nombres de columnas esperadas
    # Esperado: vehiculo_label, modelo, promedio_calificacion, n_resenas
    if "vehiculo_label" not in df.columns:
        # fallback por si se llama distinto
        for c in df.columns:
            if "vehiculo" in c.lower():
                df = df.rename(columns={c: "vehiculo_label"})
                break

    # numéricos
    for col in ["promedio_calificacion", "modelo", "n_resenas"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "n_resenas" in df.columns:
        df["n_resenas"] = df["n_resenas"].fillna(0).astype(int)

    if "promedio_calificacion" in df.columns:
        df["promedio_calificacion"] = df["promedio_calificacion"].fillna(0.0)

    return df


def cargar_calificaciones():
    if not os.path.exists(CSV_CALIF):
        raise RuntimeError(f"No encuentro el CSV de calificaciones en: {CSV_CALIF}")

    df = pd.read_csv(CSV_CALIF)

    # columna de estrellas
    col_est = None
    for c in df.columns:
        if "estrella" in c.lower():
            col_est = c
            break
    if col_est is None:
        raise RuntimeError("No encuentro columna de estrellas en calificacionesv2.csv")

    df["estrellas_num"] = pd.to_numeric(df[col_est], errors="coerce")

    # columna de fecha (para gráfico por mes)
    col_fecha = None
    for c in df.columns:
        if c.lower().startswith("fecha"):
            col_fecha = c
            break

    if col_fecha is not None:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
    df["_col_estrellas"] = col_est
    df["_col_fecha"] = col_fecha

    return df


def cargar_resenas():
    if not os.path.exists(CSV_RESENAS):
        raise RuntimeError(f"No encuentro el CSV de reseñas en: {CSV_RESENAS}")

    df = pd.read_csv(CSV_RESENAS)

    # columnas esperadas: username, idVehiculo, comentario, fecha
    if "username" not in df.columns:
        raise RuntimeError("resenas.csv debe tener columna 'username'.")

    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    return df


# ===========================
# VISTAS
# ===========================
@app.route("/dashboard")
def dashboard():
    vista = request.args.get("vista", "vehiculos")

    # ----------------------------------
    # VISTA VEHÍCULOS
    # ----------------------------------
    if vista == "vehiculos":
        df = cargar_vehiculos()
        q = request.args.get("q", "").strip()
        orden = request.args.get("orden", "mejor")

        df_filtrado = df.copy()

        # Filtro por texto (marca / modelo / nombre)
        if q:
            q_low = q.lower()
            # usamos vehiculo_label; si no existe, usamos la primera columna string
            col_nombre = "vehiculo_label"
            if col_nombre not in df_filtrado.columns:
                col_nombre = df_filtrado.columns[0]
            df_filtrado = df_filtrado[
                df_filtrado[col_nombre].astype(str).str.lower().str.contains(q_low, na=False)
            ]

        # Orden
        if orden == "mejor":
            df_filtrado = df_filtrado.sort_values(
                ["promedio_calificacion", "n_resenas"],
                ascending=[False, False],
                na_position="last"
            )
        elif orden == "peor":
            df_filtrado = df_filtrado.sort_values(
                ["promedio_calificacion", "n_resenas"],
                ascending=[True, False],
                na_position="last"
            )
        elif orden == "mas_resenas":
            df_filtrado = df_filtrado.sort_values(
                ["n_resenas", "promedio_calificacion"],
                ascending=[False, False],
                na_position="last"
            )
        elif orden == "menos_resenas":
            df_filtrado = df_filtrado.sort_values(
                ["n_resenas", "promedio_calificacion"],
                ascending=[True, False],
                na_position="last"
            )

        # Datos tabla
        registros = df_filtrado.to_dict(orient="records")

        # Datos para gráficas (top 20 por rating & por reseñas)
        top_rating = df_filtrado.head(20)
        top_resenas = df_filtrado.sort_values("n_resenas", ascending=False).head(20)

        return render_template(
            "dashboard.html",
            vista="vehiculos",
            total_autos=len(df),
            vehiculos=registros,
            search=q,
            orden=orden,
            # gráfica top rating
            veh_chart_labels=[str(v) for v in top_rating["vehiculo_label"]],
            veh_chart_scores=[float(v) for v in top_rating["promedio_calificacion"]],
            veh_chart_resenas=[int(v) for v in top_rating["n_resenas"]],
            # gráfica top reseñas
            veh_resenas_labels=[str(v) for v in top_resenas["vehiculo_label"]],
            veh_resenas_counts=[int(v) for v in top_resenas["n_resenas"]],
        )

    # ----------------------------------
    # VISTA CALIFICACIONES
    # ----------------------------------
    elif vista == "calificaciones":
        df = cargar_calificaciones()
        col_est = df["_col_estrellas"].iloc[0]
        col_fecha = df["_col_fecha"].iloc[0]

        total_calif = len(df)
        promedio_global = round(float(df["estrellas_num"].mean()), 2)

        # Distribución 1–5
        dist = (
            df["estrellas_num"]
            .round()
            .value_counts()
            .reindex([1, 2, 3, 4, 5], fill_value=0)
            .sort_index()
        )
        dist_labels = [int(x) for x in dist.index]
        dist_values = [int(v) for v in dist.values]

        # Calificaciones por mes (conteo y promedio)
        mes_labels = []
        mes_count = []
        mes_avg = []
        if col_fecha is not None:
            df_valid = df.dropna(subset=[col_fecha])
            if not df_valid.empty:
                df_valid["anio_mes"] = df_valid[col_fecha].dt.to_period("M").astype(str)
                g = (
                    df_valid.groupby("anio_mes")["estrellas_num"]
                    .agg(["size", "mean"])
                    .reset_index()
                    .sort_values("anio_mes")
                )
                mes_labels = g["anio_mes"].tolist()
                mes_count = [int(v) for v in g["size"].tolist()]
                mes_avg = [round(float(v), 2) for v in g["mean"].tolist()]

        return render_template(
            "dashboard.html",
            vista="calificaciones",
            total_calif=total_calif,
            promedio_global=promedio_global,
            dist_labels=dist_labels,
            dist_values=dist_values,
            mes_labels=mes_labels,
            mes_count=mes_count,
            mes_avg=mes_avg,
        )

    # ----------------------------------
    # VISTA RESEÑAS (TOP USUARIOS)
    # ----------------------------------
    elif vista == "reseñas":
        df = cargar_resenas()

        # Agrupar por usuario
        g = (
            df.groupby("username")["comentario"]
            .agg(["size"])
            .reset_index()
            .rename(columns={"size": "n_resenas"})
            .sort_values("n_resenas", ascending=False)
        )

        total_resenas = len(df)
        total_usuarios = g["username"].nunique()
        promedio_resenas_usuario = round(float(total_resenas / max(total_usuarios, 1)), 2)

        top_users = g.head(20)

        return render_template(
            "dashboard.html",
            vista="reseñas",
            total_resenas=total_resenas,
            total_usuarios=total_usuarios,
            promedio_resenas_usuario=promedio_resenas_usuario,
            usuarios=top_users.to_dict(orient="records"),
            res_users_labels=top_users["username"].tolist(),
            res_users_counts=[int(v) for v in top_users["n_resenas"].tolist()],
        )

    # fallback
    return "Vista no soportada", 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=False)
