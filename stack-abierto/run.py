#!/usr/bin/env python3
"""
run.py — reconstruye el modelo en DuckDB y verifica los controles de calidad.

    pip install duckdb
    python stack-abierto/run.py                    # usa ./train.csv del repo
    python stack-abierto/run.py ruta/al/archivo.csv

Sale con codigo 1 si algun control de calidad falla, asi que sirve en CI.
"""
from __future__ import annotations

import pathlib
import sys

try:
    import duckdb
except ImportError:
    sys.exit("Falta duckdb. Instalalo con:  pip install duckdb")

HERE = pathlib.Path(__file__).resolve().parent
SQL_FILE = HERE / "build.sql"


def fmt_rows(cur) -> str:
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    out = ["| " + " | ".join(cols) + " |",
           "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out)


def main() -> int:
    csv = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "train.csv"
    if not csv.exists():
        return fail(f"No encuentro el CSV: {csv}")

    sql = SQL_FILE.read_text(encoding="utf-8").replace("{{CSV}}", csv.as_posix())
    con = duckdb.connect()  # en memoria: no deja artefactos en el repo
    con.execute(sql)

    print(f"# Retail Global Store · modelo en DuckDB\n\nFuente: `{csv.name}`\n")

    dq = con.execute("SELECT * FROM dq_checks").fetchone()
    names = [d[0] for d in con.description]
    checks = dict(zip(names, dq))
    print("## Controles de calidad\n")
    for k, v in checks.items():
        print(f"- `{k}`: **{v}**")

    failures = []
    if checks["fechas_invalidas"]:
        failures.append(f"{checks['fechas_invalidas']} fechas ilegibles")
    if checks["ventas_invalidas"]:
        failures.append(f"{checks['ventas_invalidas']} ventas no numericas")
    if checks["ventas_negativas"]:
        failures.append(f"{checks['ventas_negativas']} ventas negativas")
    if checks["envio_antes_de_pedido"]:
        failures.append(f"{checks['envio_antes_de_pedido']} envios previos al pedido")
    if checks["hechos_sin_dimension"]:
        failures.append(f"{checks['hechos_sin_dimension']} hechos sin dimension")

    for title, view in [
        ("Ventas por modo de envio", "kpi_ship_mode"),
        ("Evolucion mensual", "kpi_monthly"),
        ("Top categorias", "kpi_category"),
        ("Ventas por segmento", "kpi_customer"),
    ]:
        cur = con.execute(f"SELECT * FROM {view}")
        print(f"\n## {title}\n")
        print(fmt_rows(cur))

    if failures:
        return fail("Controles fallidos: " + "; ".join(failures))
    print("\nOK · todos los controles pasan.")
    return 0


def fail(msg: str) -> int:
    print(f"\nFALLO · {msg}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
