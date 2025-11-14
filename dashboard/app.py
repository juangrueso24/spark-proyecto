import os
import pandas as pd
from flask import Flask, render_template, request

app = Flask(__name__)

# ===========================
# RUTAS DE ARCHIVOS (usan data/)
# ===========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))      # .../spark-proyecto/dashboard
ROOT_DIR = os.path.dirname(BASE_DIR)                       # .../spark-proyecto

CSV_VEHICULOS   = os.path.join(ROOT_DIR, "data", "vehiculos_agg.csv")
CSV_CALIF       = os.path.join(ROOT_DIR, "data", "calificacionesv2.csv")
CSV_RESENAS     = os.path.join(ROOT_DIR, "data", "resenas.csv")
CSV_USUARIOS    = os.path.join(ROOT_DIR, "data", "usuarios.csv")
CSV_AUTOS_RAW   = os.path.join(ROOT_DIR, "data", "autos_limpiov8.csv")  # <-- dataset grande (~5000)


# ===========================
# HELPERS
# ===========================
def cargar_vehiculos():
    if not os.path.exists(CSV_VEHICULOS):
        raise RuntimeError(f"No encuentro el CSV de vehículos en: {CSV_VEHICULOS}")

    df = pd.read_csv(CSV_VEHICULOS)

    # Asegurar columna vehiculo_label
    if "vehiculo_label" not in df.columns:
        candidatos = [
            c for c in df.columns
            if any(tok in c.lower() for tok in ["vehiculo", "vehículo", "vehicle", "modelo", "nombre"])
        ]
        if candidatos:
            df = df.rename(columns={candidatos[0]: "vehiculo_label"})
        else:
            col_obj = df.select_dtypes(include=["object"]).columns
            if len(col_obj) > 0:
                df["vehiculo_label"] = df[col_obj[0]]
            else:
                df["vehiculo_label"] = "Vehículo"

    # Asegurar columnas numéricas
    if "promedio_calificacion" not in df.columns:
        for c in df.columns:
            if "promedio" in c.lower() or "rating" in c.lower():
                df = df.rename(columns={c: "promedio_calificacion"})
                break

    if "n_resenas" not in df.columns:
        for c in df.columns:
            if "resenas" in c.lower() or "reseñas" in c.lower():
                df = df.rename(columns={c: "n_resenas"})
                break

    df["promedio_calificacion"] = pd.to_numeric(
        df.get("promedio_calificacion", 0), errors="coerce"
    ).fillna(0.0)

    df["n_resenas"] = pd.to_numeric(
        df.get("n_resenas", 0), errors="coerce"
    ).fillna(0).astype(int)

    return df


def calcular_total_autos_reales():
    """
    Devuelve el total de autos del dataset grande (autos_limpiov8.csv).
    Si por algún motivo no existe o falla, usa el tamaño de vehiculos_agg.csv.
    """
    # primero intentamos leer el CSV grande
    if os.path.exists(CSV_AUTOS_RAW):
        try:
            df_raw = pd.read_csv(CSV_AUTOS_RAW)
            return len(df_raw)
        except Exception:
            pass

    # fallback: contar filas del agregado
    if os.path.exists(CSV_VEHICULOS):
        try:
            df = pd.read_csv(CSV_VEHICULOS)
            return len(df)
        except Exception:
            pass

    return 0


def cargar_calificaciones():
    if not os.path.exists(CSV_CALIF):
        raise RuntimeError(f"No encuentro el CSV de calificaciones en: {CSV_CALIF}")

    df = pd.read_csv(CSV_CALIF)

    # Columna de estrellas (buscamos algo que contenga "estrella")
    col_est = None
    for c in df.columns:
        if "estrella" in c.lower():
            col_est = c
            break
    if col_est is None:
        raise RuntimeError("No encuentro columna de estrellas en calificacionesv2.csv")

    df["estrellas_num"] = pd.to_numeric(df[col_est], errors="coerce")

    # Columna de fecha (por si luego se usa)
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

    # columnas esperadas: username, comentario, fecha (idVehiculo si existe)
    if "username" not in df.columns:
        raise RuntimeError("resenas.csv debe tener columna 'username'.")

    if "comentario" not in df.columns:
        for c in df.columns:
            if "coment" in c.lower():
                df = df.rename(columns={c: "comentario"})
                break

    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    return df


# ===========================
# VISTA PRINCIPAL /dashboard
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

        registros = df_filtrado.to_dict(orient="records")

        top_rating = df_filtrado.head(20)
        top_resenas = df_filtrado.sort_values("n_resenas", ascending=False).head(20)

        # 🔢 ESTE es el número que quieres que diga "5000 autos reales"
        total_autos_reales = calcular_total_autos_reales()

        return render_template(
            "dashboard.html",
            vista="vehiculos",
            total_autos=total_autos_reales,   # <- se usa en el título
            vehiculos=registros,
            search=q,
            orden=orden,
            veh_chart_labels=[str(v) for v in top_rating["vehiculo_label"]],
            veh_chart_scores=[float(v) for v in top_rating["promedio_calificacion"]],
            veh_chart_resenas=[int(v) for v in top_rating["n_resenas"]],
            veh_resenas_labels=[str(v) for v in top_resenas["vehiculo_label"]],
            veh_resenas_counts=[int(v) for v in top_resenas["n_resenas"]],
        )

    # ----------------------------------
    # VISTA CALIFICACIONES
    # ----------------------------------
    elif vista == "calificaciones":
        df = cargar_calificaciones()

        total_calif = len(df)
        promedio_global = round(float(df["estrellas_num"].mean()), 2)

        dist = (
            df["estrellas_num"]
            .round()
            .value_counts()
            .reindex([1, 2, 3, 4, 5], fill_value=0)
            .sort_index()
        )
        dist_labels = [int(x) for x in dist.index]
        dist_values = [int(v) for v in dist.values]

        return render_template(
            "dashboard.html",
            vista="calificaciones",
            total_calif=total_calif,
            promedio_global=promedio_global,
            dist_labels=dist_labels,
            dist_values=dist_values,
        )

    # ----------------------------------
    # VISTA RESEÑAS
    # ----------------------------------
    elif vista == "reseñas":
        df = cargar_resenas()

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
