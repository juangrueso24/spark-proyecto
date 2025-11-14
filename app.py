import os
import pandas as pd
from flask import Flask, render_template, request

app = Flask(__name__)

# CSV generado por Spark: .../spark-proyecto/data/vehiculos_agg.csv
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
CSV_PATH = os.path.join(ROOT_DIR, "data", "vehiculos_agg.csv")


def cargar_vehiculos():
    if not os.path.exists(CSV_PATH):
        raise RuntimeError(f"No encuentro el CSV generado por Spark en: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)

    # Normalizamos por si acaso
    for col in ["modelo", "promedio_calificacion"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "n_resenas" in df.columns:
        df["n_resenas"] = pd.to_numeric(df["n_resenas"], errors="coerce").fillna(0).astype(int)

    return df


@app.route("/dashboard")
def dashboard():
    vista = request.args.get("vista", "vehiculos")

    df = cargar_vehiculos()

    if vista == "vehiculos":
        # -------- Búsqueda por marca/modelo (vehiculo_label) --------
        search = request.args.get("q", "").strip()
        if search:
            df = df[df["vehiculo_label"].str.contains(search, case=False, na=False)]

        # -------- Orden --------
        order = request.args.get("orden", "promedio_calificacion")
        if order not in df.columns:
            order = "promedio_calificacion"

        sentido = request.args.get("sentido", "desc")
        ascending = (sentido == "asc")

        df = df.sort_values(by=order, ascending=ascending)

        # -------- Top N para la gráfica --------
        try:
            top_n = int(request.args.get("top", "20"))
        except ValueError:
            top_n = 20
        top_n = max(1, min(top_n, 50))

        top_df = df.head(top_n)
        chart_labels = top_df["vehiculo_label"].tolist()
        chart_values = top_df["promedio_calificacion"].tolist()

        total = len(df)

        # Para la tabla solo mostramos primeros 500 para no matar el navegador
        vehiculos_tabla = df.head(500).to_dict(orient="records")

        return render_template(
            "dashboard.html",
            vista=vista,
            total_vehiculos=total,
            vehiculos=vehiculos_tabla,
            chart_labels=chart_labels,
            chart_values=chart_values,
            order=order,
            sentido=sentido,
            top=top_n,
            search=search,
        )

    # Otras vistas (calificaciones, reseñas) -> por ahora muestran misma tabla
    total = len(df)
    vehiculos_tabla = df.head(500).to_dict(orient="records")
    return render_template(
        "dashboard.html",
        vista=vista,
        total_vehiculos=total,
        vehiculos=vehiculos_tabla,
        chart_labels=[],
        chart_values=[],
        order="promedio_calificacion",
        sentido="desc",
        top=20,
        search="",
    )


if __name__ == "__main__":
    print("Usando CSV:", CSV_PATH)
    app.run(host="0.0.0.0", port=8081, debug=False)
