import pandas as pd, os, glob

BASE = os.path.expanduser('~/spark-proyecto')
os.makedirs(BASE, exist_ok=True)

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def cargar_catalogo_principal():
    patrones = [
        os.path.join(BASE, 'autos_limpiov8*.csv'),
        os.path.expanduser('~/Downloads/autos_limpiov8*.csv'),
    ]
    archivos = []
    for pat in patrones:
        archivos.extend(glob.glob(pat))
    if not archivos:
        return None

    df = pd.concat([pd.read_csv(p) for p in archivos], ignore_index=True)

    col_id  = pick_col(df, ['id_vehiculo','vehiculo','id_auto','id']) or df.columns[0]
    col_m   = pick_col(df, ['marca','brand','fabricante','make'])
    col_nom = pick_col(df, ['nombre_vehiculo','nombre','modelo','vehiculo_nombre','description','descripcion'])

    keep = {col_id: 'id_vehiculo'}
    if col_m:   keep[col_m]   = 'marca'
    if col_nom: keep[col_nom] = 'nombre_vehiculo'

    cat = df[list(keep.keys())].rename(columns=keep).drop_duplicates()
    for c in ['marca','nombre_vehiculo']:
        if c in cat.columns:
            cat[c] = cat[c].astype(str).str.strip()
    return cat

def main():
    out = os.path.join(BASE, 'vehiculos_catalogo.csv')
    cat = cargar_catalogo_principal()
    if cat is None or cat.empty:
        # placeholder vacío para no romper el pipeline
        pd.DataFrame(columns=['id_vehiculo','marca','nombre_vehiculo']).to_csv(out, index=False)
        print(f"[WARN] No encontré autos_limpiov8*.csv. Dejé placeholder vacío en {out}")
        return
    cat.to_csv(out, index=False)
    print(f"[OK] Catálogo creado con {len(cat):,} filas → {out}")

if __name__ == '__main__':
    main()
