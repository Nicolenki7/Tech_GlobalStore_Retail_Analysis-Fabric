# Tech_GlobalStore_Retail_Analysis-Fabric

# 🚀 Proyecto de Data Engineering: Dashboard de Ventas Globales en Microsoft Fabric (Star Schema)

### Vista Previa del Dashboard

El resultado final es un informe interactivo, limpio y eficiente, diseñado para la toma de decisiones:

![Vista Previa del Dashboard Global Store](Dashboard%20Global%20Store.png)

## 🎯 Objetivo del Proyecto

Este proyecto demuestra la capacidad para diseñar, construir y desplegar una solución completa de Business Intelligence (BI) sobre una plataforma moderna (Microsoft Fabric). El objetivo principal fue transformar datos de ventas brutos y caóticos en un **Modelo de Datos Star Schema** eficiente y un dashboard interactivo, capaz de impulsar la toma de decisiones estratégicas.

---

## 🛠️ Arquitectura y Tecnologías

| Componente | Tecnología | Propósito |
| :--- | :--- | :--- |
| **ETL & Data Transformation** | **PySpark (Notebooks de Fabric)** | Procesamiento de datos, limpieza, y aplicación de lógica de negocio (corrección de unicidad y nulidad). |
| **Data Lakehouse** | **Delta Lake / OneLake** | Almacenamiento eficiente y versionado de las tablas finales (Fact y Dimensiones). |
| **Modelado de Datos** | **Power BI / Modelo Semántico de Fabric** | Creación del Star Schema (relaciones 1:N) y definición de las métricas de negocio (DAX). |
| **Visualización & BI** | **Power BI Service (Fabric)** | Creación del dashboard final interactivo y publicable. |

---

## 📝 Star Schema: Modelo de Datos

El modelo se diseñó en torno a una tabla central de hechos (`Fact_sales`) vinculada a cuatro dimensiones, garantizando la velocidad y precisión del análisis dimensional:

| Tabla de Hechos | Claves Foráneas / Métricas |
| :--- | :--- |
| `Fact_sales` | `Order_Date` (FK), `Product_ID` (FK), `Customer_ID` (FK), `Postal_Code` (FK), `Sales` (Medida) |

| Dimensiones | Clave Primaria (PK) |
| :--- | :--- |
| `Dim_Date` | `Date` |
| `Dim_Product` | `Product_ID` |
| `Dim_customer` | `Customer_ID` |
| `Dim_Geography` | `Postal_Code` |

---

## 🛑 Desafíos Críticos Resueltos (Integridad de Datos)

El mayor desafío del proyecto fue garantizar la integridad del modelo. La capa de PySpark se modificó progresivamente para resolver fallos críticos en las claves primarias (PK) que rompían las relaciones 1:N:

1.  **Duplicidad de Claves (Product & Geography):** Se resolvió mediante la función `groupBy().agg(first(...))` en PySpark para asegurar que `Product_ID` y `Postal_Code` fueran valores únicos en sus respectivas dimensiones.
2.  **Valores Nulos en Claves Primarias:** Se resolvió aplicando filtros (`.filter(col("Clave").isNotNull())`) en las dimensiones `Dim_Date` y `Dim_Geography` para eliminar cualquier registro nulo de las PK, ya que no se permiten valores en blanco en el lado 'uno' de una relación.

*(El código final que resuelve estos problemas se encuentra en el archivo `Notebook 1.py`.)*

---

## 📊 Dashboard de BI: Resultados del Análisis

El dashboard final está diseñado para la toma de decisiones, compuesto por 4 KPIs de rendimiento y 7 visualizaciones analíticas, todas interactivas a través de segmentadores de Año y Región.

### Indicadores Clave (KPIs)

* **Total Sales**
* **Average Revenue per Order**
* **Sales per Customer**
* **Orders per Customer**

### Visualizaciones Estratégicas

| Título del Gráfico | Enfoque de Análisis |
| :--- | :--- |
| **Sales by Product Category** | Analiza la rentabilidad por portafolio de productos. |
| **Historical Sales Trend** | Identifica el crecimiento interanual y la estacionalidad del negocio. |
| **Total Sales by State / Top 10 States** | Muestra el rendimiento geográfico y los *drivers* principales por ubicación. |
| **Sales Distribution by Shipping Mode** | Evalúa la eficiencia logística y el costo asociado a cada modo de envío. |
| **Revenue by Customer Segment** | Segmenta las ventas por tipo de cliente (Marketing). |
| **Customer Value and Frequency Analysis** | Utiliza un Scatter Plot avanzado para identificar a los clientes más valiosos (RFM). |

---

### 🌐 Ver el Dashboard Interactivo

Puedes explorar la solución final y la interacción en el siguiente enlace público:

➡️ **(https://app.fabric.microsoft.com/reportEmbed?reportId=ef628e11-1d49-421b-8a9d-b82867bf8d37&autoAuth=true&ctid=5153b8f5-97d1-4e1b-827f-2fb1bad4128f)**

---

### 🙋 Contribución / Contacto

* **Autor:** Nicolas Zalazar
* **LinkedIn:** (https://www.linkedin.com/in/nicolas-zalazar-63340923a)
