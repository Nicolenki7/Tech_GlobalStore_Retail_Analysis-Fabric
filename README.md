# 🏪 Global Store Retail Analysis — Star Schema on Fabric

**Enterprise BI Solution | PySpark ETL | Star Schema | Power BI Dashboard**

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-F34F21?logo=microsoft)](https://fabric.microsoft.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E3492F?logo=apachespark)](https://spark.apache.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Overview

Complete enterprise BI solution transforming chaotic raw sales data into an efficient **Star Schema** model on Microsoft Fabric. Demonstrates end-to-end data engineering with PySpark ETL, Delta Lake storage, and comprehensive Power BI analytics for retail performance tracking.

This project showcases production-ready patterns for data integrity resolution (nulls, duplicates) and business metrics implementation.

---

## 💼 Business Impact

- **Strategic Decision-Making**: 4 KPIs and 7 analytical visualizations for sales performance tracking
- **Customer Value Insights**: RFM analysis enables targeted marketing strategies
- **Logistics Efficiency**: Delivery performance metrics identify optimization opportunities
- **Data Quality Resolution**: Null/duplicate handling ensures reliable analytics

---

## 🛠️ Technical Stack

| Category | Technologies |
| :--- | :--- |
| **Platform** | Microsoft Fabric |
| **Data Engineering** | PySpark, Dataflows Gen2 |
| **Data Storage** | Delta Lake (versioned) |
| **Data Modeling** | Star Schema (Fact/Dimensions) |
| **BI & Analytics** | Power BI, DAX |
| **Query Language** | SQL, DAX |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  RETAIL ANALYTICS PIPELINE                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  RAW DATA LAYER                                             │
│  └─→ Chaotic sales data (nulls, duplicates, inconsistencies)│
│                                                              │
│  ETL LAYER (PySpark)                                        │
│  └─→ Data cleaning & validation                             │
│      - Null handling                                        │
│      - Duplicate resolution                                 │
│      - Type correction                                      │
│                                                              │
│  MODELING LAYER (Star Schema)                               │
│  └─→ Fact_Sales                                             │
│      → Dim_Customer, Dim_Product, Dim_Date, Dim_Store       │
│                                                              │
│  STORAGE LAYER (Delta Lake)                                 │
│  └─→ Versioned tables with ACID guarantees                  │
│                                                              │
│  CONSUMPTION LAYER (Power BI)                               │
│  └─→ DAX measures, interactive dashboard                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Key Features

### PySpark ETL Pipeline
- **Data Integrity**: Null/duplicate resolution
- **Type Correction**: Consistent data types across columns
- **Validation Rules**: Business logic enforcement

### Star Schema Model
| Table Type | Tables |
| :--- | :--- |
| **Fact Table** | Fact_Sales (transactions, amounts, quantities) |
| **Dimension Tables** | Dim_Customer, Dim_Product, Dim_Date, Dim_Store |

### DAX Business Metrics
- **Total Revenue**: Sum of sales amounts
- **Average Order Value**: Revenue per transaction
- **Customer Count**: Unique customers
- **Product Performance**: Top/bottom sellers

---

## 📊 Results & Metrics

| Metric | Value |
| :--- | :--- |
| **KPIs Implemented** | 4 core metrics |
| **Visualizations** | 7 analytical charts |
| **Data Quality Issues Resolved** | Nulls, duplicates, type mismatches |
| **Schema Type** | Star Schema (dimensional modeling) |

---

## 📁 Project Structure

```
Tech_GlobalStore_Retail_Analysis-Fabric/
├── data/                              # Raw and processed data files
├── notebooks/                         # PySpark ETL notebooks
├── powerbi/                           # Power BI report files
├── docs/                              # Documentation
└── README.md                          # Project documentation
```

---

## 🔧 Setup & Installation

### Prerequisites
- Microsoft Fabric capacity (F32 or higher)
- Power BI Desktop (latest version)
- Fabric workspace permissions

### Deployment Steps

```bash
# Clone the repository
git clone https://github.com/Nicolenki7/Tech_GlobalStore_Retail_Analysis-Fabric.git
cd Tech_GlobalStore_Retail_Analysis-Fabric

# 1. Create Fabric Workspace
# 2. Upload raw data to Lakehouse
# 3. Run PySpark ETL notebooks
# 4. Create Star Schema model
# 5. Import Power BI report
# 6. Configure DAX measures
```

---

## 📈 Usage

### Dashboard Features

| Visualization | Purpose |
| :--- | :--- |
| **Revenue Trend** | Sales performance over time |
| **Top Products** | Best-selling items by revenue |
| **Customer Segments** | RFM-based customer grouping |
| **Geographic Analysis** | Sales by region/store |
| **Delivery Performance** | On-time delivery metrics |

### Interactive Filters
- **Date Range**: Custom time period selection
- **Product Category**: Filter by product type
- **Store Location**: Regional analysis
- **Customer Segment**: RFM tier filtering

---

## 🎯 Key Learnings

- **Star Schema** simplifies DAX calculations and improves query performance
- **PySpark** efficiently handles large-scale data cleaning operations
- **Delta Lake** provides versioning and ACID guarantees for production pipelines
- **RFM Analysis** enables actionable customer segmentation

---

## 🔮 Future Enhancements

- [ ] Real-time sales ingestion (KQL Database)
- [ ] Predictive inventory recommendations (ML)
- [ ] Customer churn prediction model
- [ ] Automated data quality monitoring
- [ ] Mobile-optimized dashboard layout

---

## 🔗 Links

| Resource | URL |
| :--- | :--- |
| **Repository** | https://github.com/Nicolenki7/Tech_GlobalStore_Retail_Analysis-Fabric |
| **Live Dashboard** | [View in Fabric](https://app.fabric.microsoft.com/reportEmbed?reportId=ef628e11-1d49-421b-8a9d-b82867bf8d37) |

---

## 📝 Resumen en Español

Solución completa de BI empresarial que transforma datos de ventas caóticos en un modelo **Star Schema** eficiente usando **PySpark ETL** y **Delta Lake**. Incluye 4 KPIs principales, 7 visualizaciones analíticas, y análisis RFM para segmentación de clientes. El dashboard permite seguimiento de rendimiento de ventas, análisis geográfico, y métricas de eficiencia logística.

---

## 📄 License

MIT License — Feel free to fork, modify, and use for personal or commercial projects.

---

## 👤 Author

**Nicolás Zalazar** | Senior Data Engineer & Microsoft Fabric Specialist

- GitHub: [@Nicolenki7](https://github.com/Nicolenki7)
- LinkedIn: [nicolas-zalazar-63340923a](https://www.linkedin.com/in/nicolas-zalazar-63340923a)
- Portfolio: [nicolenki7.github.io/Portfolio](https://nicolenki7.github.io/Portfolio/)
- Email: zalazarn046@gmail.com

---

*Last Updated: March 2026*
