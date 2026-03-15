from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from docker.types import Mount
import os

# --- CONFIGURATION ---
HOST_PROJECTS_PATH = '/home/e5081/bytevault/codespace/02-data-eng/pipelines'
HOST_DATA_PATH = '/home/e5081/bytevault/codespace/01-platform/Shared-Data'

# Connection Constants for the bytevault-net bridge
JDBC_URL = "jdbc:postgresql://shared-pg:5432/warehouse"
DB_USER = "admin"
DB_PASS = "admin"

default_args = {
    'owner': 'data_eng',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'medallion_inventory_pipeline',
    default_args=default_args,
    description='End-to-end Bronze to Gold Spark Pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:

    spark_image = 'easewithdata/pyspark-jupyter-lab:latest'
    
    project_mount = Mount(source=HOST_PROJECTS_PATH, target='/home/jovyan/projects', type='bind')
    data_mount = Mount(source=HOST_DATA_PATH, target='/home/jovyan/data', type='bind')

    # Centralized Spark configuration for Spark 3.3.0 + Delta 2.2.0
    spark_submit_base = [
        "spark-submit",
        "--packages", "org.postgresql:postgresql:42.7.2,io.delta:delta-core_2.12:2.2.0",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ]

    # NEW TASK: Setup source data in Postgres if it doesn't exist
    bootstrap_source = DockerOperator(
        task_id='bootstrap_source',
        image='postgres:15',
        command=f"psql {JDBC_URL.replace('jdbc:postgresql', 'postgresql')} -U {DB_USER} -c "
                "\"CREATE TABLE IF NOT EXISTS inventory (item_id SERIAL PRIMARY KEY, item_name TEXT, category TEXT, price NUMERIC); "
                "INSERT INTO inventory (item_name, category, price) VALUES ('Laptop', 'Electronics', 1200), ('Chair', 'Furniture', 150) ON CONFLICT DO NOTHING;\"",
        environment={'PGPASSWORD': DB_PASS},
        docker_url="unix://var/run/docker.sock",
        network_mode="bytevault-net",
        auto_remove='success',
    )

# Task 1: Bronze Ingestion (Optimized)
    extract_bronze = DockerOperator(
        task_id='extract_bronze',
        image=spark_image,
        command=spark_submit_base + [
            "/home/jovyan/projects/inventory_pipeline/scripts/extract_bronze.py",
            DB_USER, DB_PASS, JDBC_URL
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="bytevault-net",
        mounts=[project_mount, data_mount],
        auto_remove='success',
        mount_tmp_dir=False,
        # ADD THESE THREE LINES:
        timeout=600,                # Give Spark 10 mins to finish downloading jars
        mem_limit='2g',             # Ensure the container doesn't get OOM killed
        retries=0,                  # Temporary: set to 0 to see the final error clearly
        tty=False,                  # Set to False in Airflow to prevent buffer hangs
    )

    # Task 2: Silver Transformation
    transform_silver = DockerOperator(
        task_id='transform_silver',
        image=spark_image,
        command=spark_submit_base + [
            "/home/jovyan/projects/inventory_pipeline/scripts/transform_silver.py"
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="bytevault-net",
        mounts=[project_mount, data_mount],
        auto_remove='success',
        mount_tmp_dir=False,
        tty=True,
    )

    # Task 3: Gold Aggregation
    aggregate_gold = DockerOperator(
        task_id='aggregate_gold',
        image=spark_image,
        command=spark_submit_base + [
            "/home/jovyan/projects/inventory_pipeline/scripts/aggregate_gold.py"
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="bytevault-net",
        mounts=[project_mount, data_mount],
        auto_remove='success',
        mount_tmp_dir=False,
        tty=True,
    )

    # Task 4 & 5 (Placeholders/Updates)
    validate_gold = DockerOperator(
        task_id='validate_gold',
        image=spark_image,
        command=spark_submit_base + ["/home/jovyan/projects/inventory_pipeline/scripts/validate_gold.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bytevault-net",
        mounts=[project_mount, data_mount],
        auto_remove='success',
    )

    export_gold = DockerOperator(
        task_id='export_gold',
        image=spark_image,
        command=spark_submit_base + ["/home/jovyan/projects/inventory_pipeline/scripts/export_gold.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bytevault-net",
        mounts=[project_mount, data_mount],
        auto_remove='success',
    )

    # Define Dependencies
    bootstrap_source >> extract_bronze >> transform_silver >> aggregate_gold >> validate_gold >> export_gold
