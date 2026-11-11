# Stack abierto · el mismo modelo sin Fabric

Este proyecto se construyó originalmente en **Microsoft Fabric** (lakehouse, PySpark y Power BI).
Esa implementación sigue documentada en el repo: el `.pbix`, las capturas y el PDF de la solución.

Como el entorno de Fabric ya no está disponible, acá está la **misma arquitectura en herramientas abiertas**:
un esquema estrella que se reconstruye desde el CSV del repositorio y corre en un portátil, sin cuenta ni licencia.

## Cómo ejecutarlo

```bash
pip install duckdb
python stack-abierto/run.py
```

Sin argumentos usa `train.csv` de la raíz. También acepta otra ruta:

```bash
python stack-abierto/run.py /ruta/a/train.csv
```

La base se crea **en memoria**: no deja archivos ni carpetas extra.

## Qué construye

| Capa | En Fabric | Acá |
| :--- | :--- | :--- |
| Ingesta | Dataflows / Shortcuts | `read_csv_auto` con tipos explícitos |
| Almacenamiento | OneLake, tablas Delta | tablas en memoria de DuckDB |
| Modelo | Warehouse con esquema estrella | `fact_orders` + `dim_customer`, `dim_product`, `dim_date`, `dim_ship_mode` |
| Métricas | modelo semántico y DAX | vistas `kpi_*` con SQL estándar |
| Visualización | Power BI | los KPIs se imprimen como tablas Markdown |

## Resultado real (verificado)

Ejecutado sobre el `train.csv` versionado —9.800 líneas de pedido, 4.922 pedidos distintos—:

| ship_mode | lineas | pedidos | ventas | dias_envio |
| :--- | ---: | ---: | ---: | ---: |
| Standard Class | 5.859 | 2.945 | 1.340.831,31 | 5,01 |
| Second Class | 1.902 | 944 | 449.914,18 | 3,25 |
| First Class | 1.501 | 772 | 345.572,26 | 2,18 |
| Same Day | 538 | 261 | 125.219,04 | 0,04 |

## Controles de calidad

`run.py` termina con código 1 si algo no cuadra, así que se puede encadenar en CI:

- fechas ilegibles (`0`)
- ventas no numéricas (`0`)
- ventas negativas (`0`)
- envíos anteriores al pedido (`0`)
- hechos sin dimensión (`0`)

## Un detalle que cuesta caro

`Order Date` viene en formato **DD/MM/YYYY**, no ISO. El SQL lo convierte con una expresión regular explícita
en lugar de confiar en la inferencia automática: con `01/02/2017` la ambigüedad entre día y mes es real, y un
error ahí desplaza meses enteros del análisis.

## Qué falta para llegar a un stack completo

Este port cubre la capa de datos. Los siguientes pasos naturales son `dbt` para versionar las vistas como modelos
y Superset o Metabase para el dashboard, que es lo único que el `.pbix` sigue aportando y este script no.
