# Medallion Architecture Inventory Pipeline
* Infrastructure: Multi-container Docker environment.
* Data Lakehouse: Delta Lake integration with ACID transactions.
* Code Quality: Spark optimization (tuning memory and shuffle partitions).
* Data Governance: Validation gates and automated exports.

#### A robust Data Engineering pipeline leveraging Apache Airflow, PySpark, and Delta Lake to process inventory data through Bronze, Silver, and Gold layers.

## 🚀 Project Overview

This project automates the journey of raw inventory data from CSV format into a business-ready analytical layer. It implements a "Medallion" architecture to ensure data quality and reliability at every stage.

## Key Features:

    * Medallion Architecture: Separation of concerns using Bronze (Raw), Silver (Cleaned), and Gold (Aggregated) tables.
    
    * ACID Transactions: Powered by Delta Lake for reliable upserts (MERGE) and schema enforcement.
    
    * Orchestration: Fully managed by Apache Airflow DAGs.
    
    * Data Quality: Automated validation gates before final export.
    
    * Containerization: Entire environment runs on a multi-container Docker setup.


### 🏗️ Technical Architecture

1. Ingestion (Bronze Layer)

    Source: Raw CSV files.

    Process: Python scripts extract and load data into Delta format.

    Goal: Maintain a 1:1 raw history of the source data.

2. Transformation (Silver Layer)

    Process: Spark cleaning (standardizing categories, casting types, calculating timestamps).

    Upsert Logic: Uses Delta Lake MERGE to handle updates to existing items while inserting new ones.

3. Analytics (Gold Layer)

    Process: Aggregations (Total Inventory Value, Average Item Cost per Category).

    Goal: Provide high-performance, query-ready data for BI tools.


## 🛠️ Tech Stack

   **Component**		    **Technology**
    
    - Orchestration		    Apache Airflow

    - Processing Engine	    PySpark (Spark 3.3.0)
    
    - Storage Format		Delta Lake
    
    - Environment		    Docker & Docker Compose
    
    - Language		        Python 3.10.18



## 🚦 Pipeline Workflow (DAG)

The pipeline is orchestrated as follows:

   **Extract:** Pull raw data.
   
   **Transform:** Clean and merge into Silver.
   
   **Aggregate:** Calculate metrics for Gold.
   
   **Validate:** Run Data Quality checks (null checks, business logic validation).
   
   **Export:** Generate a stakeholder-ready CSV report.



## 📈 Key Challenges Overcome

    * Schema Drift: Resolved AnalysisException during Delta Merges by implementing explicit column mapping and schema alignment.

    * Resource Management: Optimized Spark shuffle partitions and driver memory to prevent OOM (Out of Memory) crashes in containerized environments.

    * Orchestration Reliability: Handled Docker-to-Airflow communication overhead by tuning API versions and shell syntax.


## 🏁 Getting Started

    Clone the repository.

    Start the environment: docker-compose up -d.

    Access the Airflow UI at localhost:8080.

    Trigger the medallion_inventory_pipeline DAG.


---
#### Be sure to **stargaze** the repository and check out [My GitHub Page](https://singhajeet79.github.io/) to contact me. Thank you!
