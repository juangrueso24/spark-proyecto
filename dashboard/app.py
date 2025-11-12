from flask import Flask, render_template, request
import pandas as pd, os, datetime
import plotly.express as px

BASE = os.path.expanduser('~/spark-proyecto')
app = Flask(__name__)

# ---------------- utils ----------------
def mtime_str(p):
    try:
        t = os.path.getmtime(p)
        return datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "—"

def load_csv_safe(name):
    p = os.path.join(BASE, name)
    return (pd.read_csv(p), p) if os.path.exists(p) else (None, p)

def pick_col(df, candidates, default=None):
    for c in candidates:
        if c in df.columns:
            return c
    return default

# ---------------- routes ----------------
@app.route('/')
def home():
    return '<h2>Dashboard PySpark — <a href="/dashboard">Abrir</a></h2>'

@app.route('/dashboard')
def dashboard():
    vista = (request.args.get('vista') or 'vehiculos').lower()
    charts, notas, files = {}, [], []
    table_title, table_html = "", ""

    # ========== CALIFICACIONES (sin cambios) ==========
    if vista == 'calificaciones':
        d, p = load_csv_safe('distribucion_estrellas.csv')
        files.append(('distribucion_estrellas.csv', mtime_str(p)))
        if d is not None and len(d):
            col_star = pick_col(d, ['calificacion', 'estrellas', 'rating', 'star'])
            col_n    = pick_col(d, ['n', 'count', 'total'])
            if col_star and col_n:
                charts['fig1'] = px.bar(
                    d, x=col_star, y=col_n, title='Distribución de calificaciones'
                ).to_html(full_html=False, include_plotlyjs='cdn')
        m, pm = load_csv_safe('calificaciones_mensual.csv')
        if m is not None and len(m):
            files.append(('calificaciones_mensual.csv', mtime_str(pm)))
            col_x = pick_col(m, ['ym', 'mes', 'month'])
            col_y = pick_col(m, ['n_calif', 'n', 'count', 'total'])
            if col_x and col_y:
                charts['fig2'] = px.line(
                    m, x=col_x, y=col_y, markers=True, title='Calificaciones por mes'
                ).to_html(full_html=False, include_plotlyjs=False)
        notas.append("Para actualizar esta vista: VISTA=calificaciones  python3 analisis_spark.py")

    # ========== VEHÍCULOS (ahora con búsqueda por ID y gráficas del carro) ==========
    elif vista == 'vehiculos':
        v, pv = load_csv_safe('vehiculos_agg.csv')
        if v is None or not len(v):
            notas.append("No encontré vehiculos_agg.csv. Ejecuta: VISTA=vehiculos  python3 analisis_spark.py")
        else:
            files.append(('vehiculos_agg.csv', mtime_str(pv)))

            # Mapeo flexible de columnas
            col_id   = pick_col(v, ['id_vehiculo', 'vehiculo', 'modelo']) or v.columns[0]
            col_rate = pick_col(v, ['promedio_calificacion', 'rating_promedio', 'avg_rating']) or v.columns[1]
            col_nr   = pick_col(v, ['n_resenas', 'resenas', 'reviews'])
            col_nc   = pick_col(v, ['n_calificaciones', 'calificaciones', 'votes'])

            # Parámetros de la UI
            n      = int(request.args.get('n', 20))
            order  = (request.args.get('order') or col_rate).lower()
            q      = (request.args.get('q') or '').strip()          # filtro contiene
            id_ex  = (request.args.get('id') or '').strip()         # ID exacto

            df = v.copy()

            # Filtro contiene (q)
            if q:
                df = df[df[col_id].astype(str).str.contains(q, case=False, na=False)]

            # ----- Vista DETALLE del carro (id exacto)
            if id_ex:
                df_car = v[v[col_id].astype(str) == id_ex]
                if not len(df_car):
                    notas.append(f"⚠️ No encontré el id '{id_ex}' en {col_id}.")
                else:
                    row = df_car.iloc[0]
                    # Gráfica 1: barra del rating del carro (0..5)
                    charts['fig1'] = px.bar(
                        pd.DataFrame({
                            'métrica':['promedio_calificacion'],
                            'valor':[row[col_rate]]
                        }),
                        x='métrica', y='valor',
                        title=f'Carro {id_ex} — Promedio de calificación'
                    ).update_yaxes(range=[0,5]).to_html(full_html=False, include_plotlyjs='cdn')

                    # Gráfica 2: comparación de conteos
                    comp = pd.DataFrame({
                        'métrica':['n_calificaciones','n_resenas'],
                        'valor':[row[col_nc] if col_nc in v.columns else 0,
                                row[col_nr] if col_nr in v.columns else 0]
                    })
                    charts['fig2'] = px.bar(
                        comp, x='métrica', y='valor',
                        title=f'Carro {id_ex} — Volumen (calificaciones vs reseñas)'
                    ).to_html(full_html=False, include_plotlyjs=False)

                    # Ranking del carro en el total por dos criterios
                    df_rank_r = v.sort_values(col_rate, ascending=False).reset_index(drop=True)
                    pos_rate = int(df_rank_r.index[df_rank_r[col_id].astype(str)==id_ex][0]) + 1
                    df_rank_c = v.sort_values(col_nc if col_nc in v.columns else col_rate, ascending=False).reset_index(drop=True)
                    pos_cnt  = int(df_rank_c.index[df_rank_c[col_id].astype(str)==id_ex][0]) + 1
                    notas.append(f"Ranking por promedio: #{pos_rate} de {len(v)} — Ranking por conteo: #{pos_cnt} de {len(v)}")

                    # Tabla con la fila del carro
                    cols = [c for c in [col_id, col_rate, col_nc, col_nr] if c in v.columns]
                    table_title = f"Detalle — id {id_ex}"
                    table_html  = df_car[cols].to_html(index=False, classes="grid")

            # ----- Vista TOP (sin id exacto)
            if not id_ex:
                # Orden
                if order in df.columns:
                    df = df.sort_values(order, ascending=False)
                else:
                    df = df.sort_values(col_rate, ascending=False)

                df_top = df.head(n).copy()
                df_top['vehiculo_label'] = df_top[col_id].astype(str)

                charts['fig1'] = px.bar(
                    df_top, x='vehiculo_label', y=col_rate,
                    title=f'Top {n} vehículos — orden: {order}',
                ).to_html(full_html=False, include_plotlyjs='cdn')

                # Tabla (listado actual filtrado por q)
                cols = [c for c in [col_id, col_rate, col_nc, col_nr] if c in v.columns]
                table_title = f"Vehículos (total: {len(df)})"
                table_html  = df[cols].to_html(index=False, classes="grid")

            notas.append("TIP: en Vehículos usa ?id=11213 para ver el detalle exacto, o ?q=112 para filtrar por contiene.")

    # ========== RESEÑAS (sin cambios) ==========
    elif vista == 'resenas':
        d, pdaily = load_csv_safe('resenas_diarias.csv')
        if d is not None and len(d):
            files.append(('resenas_diarias.csv', mtime_str(pdaily)))
            xcol = pick_col(d, ['fecha_d', 'fecha', 'date', 'dia'])
            ycol = pick_col(d, ['n_resenas', 'n', 'count'])
            if xcol and ycol:
                try:
                    d[xcol] = pd.to_datetime(d[xcol])
                except Exception:
                    pass
                charts['fig1'] = px.line(
                    d, x=xcol, y=ycol, markers=True, title='Reseñas por día'
                ).to_html(full_html=False, include_plotlyjs='cdn')
        u, pu = load_csv_safe('usuarios_agg.csv')
        if u is not None and len(u):
            files.append(('usuarios_agg.csv', mtime_str(pu)))
            n = int(request.args.get('n', 10))
            user_q = (request.args.get('user') or '').strip()
            col_user = pick_col(u, ['usuario', 'username', 'user'])
            col_nu   = pick_col(u, ['n_resenas', 'n', 'count'])
            if col_user and col_nu:
                uf = u.copy()
                if user_q:
                    uf = uf[uf[col_user].astype(str).str.contains(user_q, case=False, na=False)]
                uf = uf.sort_values(col_nu, ascending=False)
                charts['fig2'] = px.bar(
                    uf.head(n), x=col_user, y=col_nu, title=f"Top {n} usuarios por reseñas"
                ).to_html(full_html=False, include_plotlyjs=False)
                table_title = f"Usuarios — {'filtrados' if user_q else 'todos'} (total: {len(uf)})"
                table_html  = uf[[col_user, col_nu]].to_html(index=False, classes="grid")
        notas.append("Para actualizar esta vista: VISTA=resenas  python3 analisis_spark.py")

    # ---------------------------------------
    return render_template(
        'dashboard.html',
        vista=vista, charts=charts, notas=notas, files=files,
        table_title=table_title, table_html=table_html
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=False)
