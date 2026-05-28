# 🌦️ End-to-End Weather Data Pipeline | Azure · Databricks · Airflow

An end-to-end **production-style data engineering project** built using Azure cloud services, Databricks, Delta Lake, and Apache Airflow.

This pipeline ingests historical weather data from the Open-Meteo API, processes it through a **Bronze → Silver → Gold** medallion architecture, and transforms it into an analytics-ready star schema with automated orchestration, incremental loading, and monitoring.

---

## 🚀 Project Highlights

- Automated daily weather data ingestion from API
- Incremental loading using watermarking
- Medallion Architecture (Bronze → Silver → Gold)
- Data transformation using PySpark & Databricks
- Delta Lake external tables with Unity Catalog
- Airflow orchestration with failure email alerts
- Star schema modelling for analytics/reporting
- Production-style folder structure & pipeline design

---

# 🏗️ Architecture Flow

```text
Open-Meteo API
        ↓
Azure Data Factory (ADF)
        ↓
ADLS Gen2 - Bronze Layer (Raw JSON)
        ↓
Databricks Notebook - Bronze → Silver
        ↓
ADLS Gen2 - Silver Layer (Clean Delta Tables)
        ↓
Databricks Notebook - Silver → Gold
        ↓
ADLS Gen2 - Gold Layer (Star Schema)
        ↓
Apache Airflow Orchestration
        ↓
Monitoring & Email Alerts
```

---

# 🧰 Tools & Technologies

| Tool / Service | Purpose |
|---|---|
| Azure Data Factory | API ingestion & orchestration |
| Azure Data Lake Gen2 | Layered storage |
| Azure Databricks | Spark transformations |
| Delta Lake | ACID table storage |
| Apache Airflow | Workflow orchestration |
| PySpark | Data transformation |
| Docker | Local Airflow setup |
| Azure Monitor | Failure alerts |
| Open-Meteo API | Weather data source |

---

# ⭐ Star Schema

### Fact Table
- `fact_weather_hourly`

### Dimension Tables
- `dim_date`
- `dim_location`
- `dim_weather_condition`

---

# 🔄 Pipeline Features

- Incremental ingestion using watermark file
- Automated daily scheduling
- Retry & monitoring mechanisms
- Failure email notifications
- Delta MERGE-based upserts
- Optimized queries using ZORDER

---

# 📁 Project Structure

```bash
End-to-End Weather Data Pipeline | Azure · Databricks · Airflow/
│
├── dags/
│   ├── utils/
│   │   ├── __init__.py
│   │   └── watermark.py
│   │
│   └── weather_pipeline_dag.py
│
├── config/
│   └── pipeline_config.py
│
├── notebooks/
│   ├── 01_bronze_to_silver
│   └── 02_silver_to_gold
│
├── control/
│   └── watermark/
│       ├── watermark.json
│       └── locations.json
│
├── screenshots/
│
└── arm template/
```

---

# 📊 Data Source

**Open-Meteo Historical Weather API**

- Hourly weather data
- 10 global cities
- No API key required
- Real-world historical weather metrics

---

# ✅ Key Outcomes

- Built a scalable cloud-native data pipeline
- Implemented incremental processing strategy
- Designed production-grade orchestration workflow
- Created analytics-ready star schema model
- Gained hands-on experience with Azure ecosystem

