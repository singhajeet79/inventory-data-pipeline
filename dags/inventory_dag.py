from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'gemini_student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'env': {'DOCKER_API_VERSION': '1.41'} # <--- Add this line
}

with DAG(
    'medallion_inventory_pipeline',
    default_args=default_args,
    description='End-to-end Bronze to Gold Spark Pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Bronze Ingestion
    # Ingestion: Python-based extraction to Bronze
    extract_bronze = BashOperator(
        task_id='extract_bronze',
        bash_command='DOCKER_API_VERSION=1.44 docker exec de-master-lab spark-submit /home/jovyan/projects/inventory_pipeline/scripts/extract_bronze.py'
    )

    # Task 2: Silver Transformation
    # Transformation: Spark-based cleaning and Upserts to Silver
    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command='DOCKER_API_VERSION=1.44 docker exec de-master-lab spark-submit /home/jovyan/projects/inventory_pipeline/scripts/transform_silver.py'
    )

    # Task 3: Gold Aggregation
    # Analytics: Business logic aggregation to Gold
    aggregate_gold = BashOperator(
        task_id='aggregate_gold',
        bash_command=(
            'DOCKER_API_VERSION=1.44 docker exec de-master-lab /bin/bash -c '
            '"spark-submit --master local --driver-memory 256m '
            '/home/jovyan/projects/inventory_pipeline/scripts/aggregate_gold.py"'
        )
    )
    # Task 4: Automated Data Quality (DQ) Check
    validate_gold = BashOperator(
        task_id='validate_gold',
        bash_command='DOCKER_API_VERSION=1.44 docker exec de-master-lab spark-submit --master "local[1]" /home/jovyan/projects/inventory_pipeline/scripts/validate_gold.py'
    )

    # Delivery: Final CSV Export for stakeholders.
    # Access the CSV => docker cp de-master-lab:/home/jovyan/data/exports/inventory_report.csv ./inventory_report.csv
    export_gold = BashOperator(
        task_id='export_gold',
        bash_command='DOCKER_API_VERSION=1.44 docker exec de-master-lab python3 /home/jovyan/projects/inventory_pipeline/scripts/export_gold.py'
    )

    # Final dependency chain
    extract_bronze >> transform_silver >> aggregate_gold >> validate_gold >> export_gold
