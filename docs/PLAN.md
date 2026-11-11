# Plan de ejecucion

## Estado actual
- `ETL_Analysis.py` (export de notebook, PySpark) + `train.csv` (9.800 filas, Superstore) + dashboard Power BI.
- `requirements.txt` y `.gitignore` anadidos.

## Proximos pasos
1. Dejar de descargar `train.csv` por HTTP en cada ejecucion: leer local, usar la URL solo como fallback.
2. Convertir el `.py` exportado a `.ipynb` real o a modulo con `def main()`.
3. Test del star schema: dimensiones, claves y granularidad de la tabla de hechos.
4. CI que ejecute el test sin Spark (muestra con pandas).

## Criterio de aceptacion
- El analisis corre en local sin cluster y el test valida el modelo dimensional.

## Riesgos
- `pyspark` en CI es pesado; los tests deben evitar el runtime completo.
