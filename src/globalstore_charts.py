"""Global Store: qué vende, cómo lo envía y con qué tendencia.

Datos: train.csv (9.800 líneas de pedido, 2015-2018). Las fechas vienen en
formato día/mes/año en el archivo original: leerlas como mes/día/año deja
5.841 de 9.800 filas sin fecha, en silencio.

Uso: python src/globalstore_charts.py
"""
import pathlib
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from svg_chart import AZUL, ROJO, barras

ROOT = pathlib.Path(__file__).resolve().parents[1]
d = pd.read_csv(ROOT / "train.csv")
IMG, OUT = ROOT / "docs" / "img", ROOT / "outputs"
IMG.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

d["fecha"] = pd.to_datetime(d["Order Date"], format="%d/%m/%Y", errors="coerce")
sin_fecha = int(d["fecha"].isna().sum())
print(f"1. {len(d):,} líneas · {d['Order ID'].nunique():,} pedidos · "
      f"{d['Customer ID'].nunique():,} clientes · fechas ilegibles {sin_fecha}")
print(f"   ventas {d['Sales'].sum():,.2f} · ticket medio por línea {d['Sales'].mean():,.2f}")

print("\n2. motor: cómo se envía")
envio = d["Ship Mode"].value_counts()
print((envio / envio.sum() * 100).round(1).to_string())
barras([(e.replace(" Class", ""), v, AZUL) for e, v in envio.items()],
       "Seis de cada diez líneas de pedido se envían por la vía estándar",
       IMG / "globalstore-envio.svg",
       subtitulo="líneas de pedido por modo de envío · 9.800 líneas entre 2015 y 2018",
       formato="{:,.0f}")

print("\n3. qué vende")
cat = (d.groupby("Category")["Sales"].sum() / d["Sales"].sum() * 100).sort_values(ascending=False)
print(cat.round(1).to_string())
barras([(c, v, AZUL) for c, v in cat.items()],
       "Las tres categorías se reparten el negocio sin dueño claro",
       IMG / "globalstore-categoria.svg",
       subtitulo="% de las ventas por categoría · ninguna pasa del 37 %",
       formato="{:,.1f}", sufijo="%")

print("\n4. tendencia anual")
anual = d.groupby(d["fecha"].dt.year)["Sales"].sum()
print(anual.round(0).to_string())
barras([(f"{int(a)}", v, AZUL) for a, v in anual.items()],
       "2016 fue el único año que cayó; después, dos años de subida",
       IMG / "globalstore-anual.svg",
       subtitulo="ventas anuales (USD) · 4.922 pedidos entre 2015 y 2018",
       formato="{:,.0f}")

d.groupby(d["fecha"].dt.to_period("M"))["Sales"].sum().to_csv(OUT / "ventas_mensuales.csv")
print(f"\n5. meses: 48 · rango {d['fecha'].min():%Y-%m} → {d['fecha'].max():%Y-%m}")
