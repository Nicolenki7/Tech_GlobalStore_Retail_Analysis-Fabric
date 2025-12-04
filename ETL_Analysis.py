# -*- coding: utf-8 -*-
# <nbformat>4.2</nbformat>

# ## Proyecto de Data Engineering: ETL y Análisis de Ventas (Microsoft Fabric / PySpark)
# 
# # Script COMPLETO y DEFINITIVO: Resuelve problemas de integridad de datos (nulidad y unicidad) y genera la réplica de visualizaciones estáticas.

import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, to_date, date_format, year, month, dayofmonth, lit, when, first, regexp_replace

# Nota: Asume que Spark está pre-inicializado (como en Fabric o Databricks).

# --- 1. CONFIGURACIÓN Y LECTURA DEL ARCHIVO CSV ---
# 🚨 RUTA CORREGIDA: Usando el enlace RAW de GitHub para asegurar la portabilidad.
# (He reemplazado el hash largo del commit por 'main' o 'master' para una URL más estable en la rama principal)
RAW_FILE_PATH = "https://raw.githubusercontent.com/Nicolenki7/Tech_GlobalStore_Retail_Analysis-Fabric/main/train.csv" 

# Lectura del CSV
df_raw = spark.read.csv(RAW_FILE_PATH, header=True, inferSchema=True)
print("✅ 1. Datos crudos leídos exitosamente desde GitHub.")

# --- 2. LIMPIEZA DE NOMBRES DE COLUMNA Y CONVERSIÓN DE TIPOS ---
df_clean = df_raw
# Estandarización de nombres:
for column in df_clean.columns:
    new_column = re.sub(r'[\s-]', '_', column) 
    df_clean = df_clean.withColumnRenamed(column, new_column)

# Conversión de tipos de datos cruciales
df_clean = df_clean.withColumn("Sales", col("Sales").cast("double")) \
                 .withColumn("Order_Date", to_date(col("Order_Date"), "MM/dd/yyyy")) \
                 .withColumn("Ship_Date", to_date(col("Ship_Date"), "MM/dd/yyyy")) \
                 .withColumn("Postal_Code", col("Postal_Code").cast("string")) \
                 .withColumn("Row_ID", col("Row_ID").cast("long"))

print("✅ 2. Tipos de datos estandarizados y nombres de columnas corregidos.")

# --- 3. CREACIÓN DE TABLAS DE DIMENSIÓN (CORRECCIÓN DE INTEGRIDAD) ---

# 🛑 DIMENSIÓN PRODUCTO (Dim_Product) - SOLUCIÓN A DUPLICIDAD
# Agrupamos por Product_ID para garantizar la unicidad de la clave.
df_dim_product = df_clean.groupBy("Product_ID").agg(
    first(col("Category")).alias("Category"),
    first(col("Sub_Category")).alias("Sub_Category"),
    first(col("Product_Name")).alias("Product_Name")
)
print("✅ Dim_Product corregida.")


# DIMENSIÓN: CLIENTE (Dim_customer)
df_dim_customer = df_clean.selectExpr(
    "Customer_ID",
    "Customer_Name",
    "Segment"
).distinct()


# 🛑 DIMENSIÓN: GEOGRAFÍA (Dim_Geography) - SOLUCIÓN A NULOS Y DUPLICIDAD
# 1. Filtramos los nulos en Postal_Code.
# 2. Agrupamos por Postal_Code para garantizar la unicidad.
df_dim_geography = df_clean.filter(col("Postal_Code").isNotNull()) \
                           .groupBy("Postal_Code").agg(
    first(col("Country")).alias("Country"),
    first(col("City")).alias("City"),
    first(col("State")).alias("State"),
    first(col("Region")).alias("Region")
)
print("✅ Dim_Geography corregida.")


# 🛑 DIMENSIÓN: FECHAS (Dim_Date) - SOLUCIÓN A NULOS
# 1. Filtramos los nulos en Order_Date.
Max_Año_Datos = df_clean.select(year(col("Order_Date"))).agg({"year(Order_Date)": "max"}).collect()[0][0]

df_dim_date = (
    df_clean.filter(col("Order_Date").isNotNull()) 
    .select(col("Order_Date").alias("Date"))
    .distinct()
    .withColumn("Año", year(col("Date")))
    .withColumn("Nombre_del_mes", date_format(col("Date"), "MMMM"))
    .withColumn("Trimestre", when(month(col("Date")) <= 3, 1)
                           .when(month(col("Date")) <= 6, 2)
                           .when(month(col("Date")) <= 9, 3)
                           .otherwise(4))
    .withColumn("Día_de_la_semana", date_format(col("Date"), "EEEE"))
    .withColumn("Es_Último_Año", when(year(col("Date")) == Max_Año_Datos, lit(True)).otherwise(lit(False)))
    .withColumnRenamed("Nombre_del_mes", "Nombre_del_mes")
)
print("✅ Dim_Date corregida.")


# --- 4. CREACIÓN DE LA TABLA DE HECHOS (Fact_Sales) ---
df_fact_sales = df_clean.selectExpr(
    "Row_ID",
    "Order_ID",
    "Order_Date",
    "Ship_Date",
    "Ship_Mode",
    "Customer_ID",
    "Product_ID",
    "Sales",
    "Postal_Code"
)

print("✅ 3. Tablas de Hechos y Dimensiones creadas.")

# --- 5. ESCRITURA FINAL COMO TABLAS DELTA (Para Fabric/Databricks) ---
# Se mantiene el código de escritura para uso en entornos de Lakehouse.
df_fact_sales.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/Fact_sales")
df_dim_product.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/Dim_Product")
df_dim_customer.write.mode("overwrite").format("delta").save("Tables/Dim_customer")
df_dim_geography.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/Dim_Geography")
df_dim_date.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/Dim_Date")

print("🎉 4. ¡Proceso ETL completado! Tablas Delta escritas/sobrescritas.")

# =========================================================================
# --- 6. VISUALIZACIÓN DE RÉPLICA (Matplotlib/Seaborn) ---
# =========================================================================

print("\n--- Generando Replicas de Visualización Estática (Matplotlib/Seaborn) ---")

# Convertimos la tabla limpia principal a Pandas para el análisis y visualización local
# Usamos un muestreo si el dataset es muy grande, pero aquí usamos toPandas() completo.
df_pd = df_clean.toPandas()

plt.style.use('seaborn-v0_8-whitegrid')
fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 18))
plt.subplots_adjust(hspace=0.5)

# --- Gráfico 1: Sales by Product Category (Gráfico de Barras) ---
sales_by_category = df_pd.groupby('Category')['Sales'].sum().sort_values(ascending=False)
sns.barplot(x=sales_by_category.index, y=sales_by_category.values, ax=axes[0], palette="Blues_d")
axes[0].set_title('Sales by Product Category', fontsize=16)
axes[0].set_xlabel('Category')
axes[0].set_ylabel('Total Sales (USD)')
axes[0].ticklabel_format(style='plain', axis='y')

# --- Gráfico 2: Historical Sales Trend (Gráfico de Líneas) ---
# Agregación por Mes/Año
df_pd['YearMonth'] = df_pd['Order_Date'].dt.to_period('M')
sales_trend = df_pd.groupby('YearMonth')['Sales'].sum()
sales_trend.index = sales_trend.index.astype(str)

sns.lineplot(x=sales_trend.index, y=sales_trend.values, ax=axes[1], color='coral')
axes[1].set_title('Historical Sales Trend (Monthly)', fontsize=16)
axes[1].set_xlabel('Time (Year-Month)')
axes[1].set_ylabel('Total Sales (USD)')
axes[1].tick_params(axis='x', rotation=45, labelsize=8)
axes[1].ticklabel_format(style='plain', axis='y') 
axes[1].locator_params(axis='x', nbins=10)

# --- Gráfico 3: Customer Value and Frequency Analysis (Scatter Plot) ---
customer_sales = df_pd.groupby('Customer_ID')['Sales'].sum().reset_index(name='Total_Sales')
customer_orders = df_pd.groupby('Customer_ID')['Order_ID'].nunique().reset_index(name='Total_Orders')

df_customer_analysis = pd.merge(customer_sales, customer_orders, on='Customer_ID')

sns.scatterplot(
    x='Total_Orders', 
    y='Total_Sales', 
    data=df_customer_analysis, 
    ax=axes[2], 
    alpha=0.6,
    color='#4CAF50'
)
axes[2].set_title('Customer Value and Frequency Analysis', fontsize=16)
axes[2].set_xlabel('Total Orders per Customer (Frequency)')
axes[2].set_ylabel('Total Sales per Customer (Value)')

plt.show()
print("🎉 Visualizaciones estáticas generadas exitosamente.")
