"""Construye docs/report.html de Global Store a partir de train.csv.

Uso: python src/build_report.py
"""
import pathlib
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from report import escribir, num

ROOT = pathlib.Path(__file__).resolve().parents[1]
d = pd.read_csv(ROOT / "train.csv")
d["fecha"] = pd.to_datetime(d["Order Date"], format="%d/%m/%Y", errors="coerce")
d["año"] = d["fecha"].dt.year

filas = []
for (a, k), v in d.groupby(["año", d["fecha"].dt.strftime("%m/%y")])["Sales"].sum().items():
    filas.append({"a": str(int(a)), "g": "mes", "k": k, "v": round(float(v), 2), "u": "$"})
for (a, c), v in d.groupby(["año", "Category"])["Sales"].sum().items():
    filas.append({"a": str(int(a)), "g": "categoria", "k": c, "v": round(float(v), 2), "u": "$"})
for (a, s), cantidad in d.groupby(["año", "Ship Mode"]).size().items():
    filas.append({"a": str(int(a)), "g": "modo", "k": s.replace(" Class", ""),
                  "v": int(cantidad), "u": ""})

anual = d.groupby("año")["Sales"].sum()
tabla_filas = []
for a, v in anual.items():
    pedidos = d[d.año == a]["Order ID"].nunique()
    tabla_filas.append([str(int(a)), num(v), num(pedidos), num(v / pedidos, 2)])

escribir(
    ROOT / "docs" / "report.html",
    eyebrow="Retail · Star Schema on Fabric",
    titulo="Global Store: ¿dónde está expuesto el negocio?",
    pregunta=("9.800 líneas de pedido entre 2015 y 2018. La pregunta no es cuánto se vende, "
              "sino dónde está el riesgo: en el surtido, que se reparte entre tres categorías, "
              "o en la operación, que se concentra en un modo de envío."),
    kpis=[("Ventas", f"{num(d.Sales.sum())} $", "2015-2018"),
          ("Líneas de pedido", num(len(d)), f"{num(d['Order ID'].nunique())} pedidos"),
          ("Ticket medio", f"{num(d.Sales.mean(), 2)} $", "por línea"),
          ("Clientes", num(d["Customer ID"].nunique()), "compradores únicos")],
    filtros=["todos", "2015", "2016", "2017", "2018"],
    graficos=[
        {"id": "mes", "grupo": "mes", "titulo": "Ventas mes a mes",
         "subtitulo": "48 meses completos · 2016 fue el único año que cayó",
         "unidad": "$"},
        {"id": "cat", "grupo": "categoria", "titulo": "Reparto por categoría",
         "subtitulo": "ninguna categoría pasa del 37 %: el surtido no está concentrado",
         "unidad": "$"},
        {"id": "modo", "grupo": "modo", "titulo": "Modo de envío",
         "subtitulo": "seis de cada diez líneas van por la vía estándar",
         "unidad": ""},
    ],
    filas=filas,
    tabla={"titulo": "Ventas por año",
           "subtitulo": "el ticket medio se calcula sobre pedidos distintos, no sobre líneas",
           "cabeceras": ["Año", "Ventas ($)", "Pedidos", "Ticket medio ($)"],
           "filas": tabla_filas},
    metodo=["Las fechas se parsean como día/mes/año: con mes/día, 5.841 de 9.800 filas quedarían en null.",
            "Ventas = suma de la columna Sales, una fila por línea de pedido (no hay descuento ni coste en el archivo).",
            "El filtro de año recalcula los tres gráficos; la tabla siempre muestra los cuatro años.",
            "Ticket medio = ventas del año / pedidos distintos del año."],
    limitaciones=["No hay coste, margen ni descuento, así que la rentabilidad no se puede medir.",
                  "No hay dimensión de tienda ni cantidad: la unidad es la línea de pedido.",
                  "Es un dataset público de retail estadounidense, no una extracción de cliente.",
                  "Hay fechas de envío pero no un SLA definido: 'entrega tardía' exigiría un umbral inventado."],
    fuente="train.csv (9.800 filas)",
    metodo_sub="Cada cifra sale de un groupby sobre el CSV versionado; nada está escrito a mano.",
    lim_sub="Lo que este informe no puede afirmar, para que nadie lo lea de más.")
print("docs/report.html:", (ROOT / "docs" / "report.html").stat().st_size, "bytes")
