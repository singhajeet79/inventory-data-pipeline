# 🏦 Medallion Architecture Inventory Pipeline

[![Tech Stack](https://img.shields.io/badge/Stack-Airflow%20|%20Spark%20|%20Delta%20Lake-blue)](#-tech-stack)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/singhajeet79)

A robust Data Engineering pipeline leveraging **Apache Airflow**, **PySpark**, and **Delta Lake** to process inventory data from transactional databases into a business-ready analytics layer.



## 🚀 Project Overview

This project automates the ETL/ELT journey of inventory data using a "Medallion" architecture. It ensures data quality, ACID compliance, and schema reliability through every stage of the lifecycle—from raw ingestion to stakeholder reporting.

### Key Features:
* **Multi-Source Ingestion:** Extracting directly from Postgres transactional systems via JDBC.
* **ACID Transactions:** Powered by **Delta Lake** for reliable Upserts (MERGE) and Schema Enforcement.
* **Orchestration:** Fully managed via Airflow DAGs with Docker-operator integration.
* **Data Governance:** Automated validation gates (Null checks, Business Logic verification) before final export.
* **Resource Optimization:** Fine-tuned Spark configurations for containerized environments.

---

## 🏗️ Technical Architecture

### 1. Ingestion (Bronze Layer)
* **Source:** PostgreSQL (Inventory Warehouse).
* **Process:** PySpark extraction with explicit type-casting for schema stability.
* **Goal:** Maintain a 1:1 raw history with added metadata (ingestion timestamps, source system tags).

### 2. Transformation (Silver Layer)
* **Process:** Data cleaning, category standardization (Uppercase), and timestamp normalization.
* **Upsert Logic:** Implements `DeltaTable.merge()` to handle change data capture (CDC), updating existing items and inserting new arrivals.

### 3. Analytics (Gold Layer)
* **Process:** Complex aggregations (Total Inventory Value, Item Tally per Category).
* **Goal:** High-performance, query-ready summary tables optimized for BI tools and Executive Reporting.

---

## 🛠️ Tech Stack

| Component | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow |
| **Processing Engine** | PySpark (Spark 3.3.0+) |
| **Storage Format** | Delta Lake (ACID compliant) |
| **Source DB** | PostgreSQL |
| **Environment** | Docker & Docker Compose |
| **Language** | Python 3.10 |

---

## 📈 Engineering Challenges Overcome

* **Type Widening & Schema Evolution:** Successfully handled `AnalysisException` during the transition from `IntegerType` to `LongType` for global identifiers by implementing explicit casting and `overwriteSchema` protocols.
* **Resource Management:** Optimized Spark shuffle partitions (`spark.sql.shuffle.partitions=1`) and driver memory (`512MB`) to ensure stability within limited Docker containers.
* **Aggregation Logic:** Refined data metrics from simple sums to categorical counts, ensuring accurate inventory reporting.

---

## 🚦 Pipeline Workflow

1.  **Extract:** Ingest raw Postgres data into Delta format.
2.  **Transform:** Clean, standardize, and MERGE into Managed Silver tables.
3.  **Aggregate:** Generate category-based metrics for Gold.
4.  **Validate:** Execute DQ checks (Null checks & Value range validation).
5.  **Export:** Produce a clean, stakeholder-ready CSV report.

---

## 🏁 Getting Started

1.  **Clone the repository.**
2.  **Start the environment:** `docker-compose up -d`.
3.  **Access Airflow UI:** Navigate to `localhost:8080`.
4.  **Trigger the DAG:** Run `medallion_inventory_pipeline`.

---
#### Check out [My GitHub Page](https://singhajeet79.github.io/) for more AI/MLOps/Data Engineering projects. 
**Happy Engineering!** 🚀
