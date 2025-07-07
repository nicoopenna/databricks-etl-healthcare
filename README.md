# 🩺 Health Status Updates Pipeline

## 🧠 Project Overview
This Azure Databricks ETL pipeline processes daily health status updates using Medallion Architecture. It ingests new Bronze layer files, upserts into Silver, and regenerates Gold layer aggregates for healthcare insights.

## 📅 Scheduling & Workflow
- **Trigger Time**: Daily at **5:00 PM**
- **Source File**: `health_status_updates` in Bronze layer
- **Upsert Key**: `status_update_id`
- **Aggregation Refresh**: Gold tables recreated from scratch every run

## ⚙️ Pipeline Breakdown
| Notebook Name             | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| `0_setup.ipynb`           | Initializes paths, databases, and mounts the container                      |
| `1_bronze_to_silver.ipynb`| Reads Bronze data and performs upsert into Silver layer                     |
| `2_silver_to_gold.ipynb`  | Aggregates Silver data into Gold layer tables                               |
| `3_master.ipynb`          | Orchestrates full pipeline execution and scheduling                         |

## 🧱 Gold Layer Aggregation
All tables are refreshed daily and stored in the `gold` folder of the `healthupdates` ADLS container, registered under the `healthcare` database.

| Table Name               | Grouped By Columns                               |
|--------------------------|--------------------------------------------------|
| `filling_count_de`       | `feeling_to_de`, `date_provided`                |
| `symptom_count_day`      | `general_symptoms`, `date_provided`             |
| `healthcare_visit_count` | `healthcare_visit`, `date_provided`             |

## 📁 Folder Structure
```plaintext
/notebooks/
  ├─ 0_setup.ipynb
  ├─ 1_bronze_to_silver.ipynb
  ├─ 2_silver_to_gold.ipynb
  └─ 3_master.ipynb

/README.md
```

## 🛠️ Technologies Used
- Azure Databricks (Delta Lake, Notebooks)
- Apache Spark (PySpark, Spark SQL)
- Azure Data Lake Storage (ADLS Gen2)
- Databricks Workflow Scheduler

## 🚀 Execution Steps
1. Drop daily `health_status_updates` file into Bronze layer
2. Run `3_master.ipynb` to execute the full pipeline
3. Schedule `3_master.ipynb` in Databricks Workflows for 5:00 PM daily execution

---
💡 *This project demonstrates modular pipeline design, schema enforcement, and daily refresh logic using cloud-native tools. Ideal for production-ready health data workflows.*
