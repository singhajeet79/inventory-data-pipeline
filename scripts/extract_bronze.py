import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

def extract_bronze():
    # Capture arguments from Airflow
    if len(sys.argv) < 4:
        print("Usage: extract_bronze.py <user> <password> <jdbc_url>")
        sys.exit(1)

    db_user = sys.argv[1]
    db_pass = sys.argv[2]
    jdbc_url = sys.argv[3]

    spark = SparkSession.builder \
        .appName("extract_bronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print(f"--- CONNECTING TO POSTGRES: {jdbc_url} ---")

    # Read from Postgres
    raw_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "inventory") \
        .option("user", db_user) \
        .option("password", db_pass) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print("--- FIXING SCHEMA AND ADDING METADATA ---")
    
    # Force item_id to LongType to match future-proof standards
    bronze_df = raw_df \
        .withColumn("item_id", col("item_id").cast("long")) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("postgres_warehouse"))

    target_path = "/home/jovyan/data/bronze/inventory"

    print(f"--- WRITING TO BRONZE DELTA TABLE: {target_path} ---")
    
    try:
        # We use overwriteSchema because the transition from Integer to Long 
        # is often blocked in 'append' mode even with mergeSchema=true.
        # This will update the table's definition to 'Long' permanently.
        bronze_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target_path)
        
        print("SUCCESS: Bronze layer updated. Schema is now set to LongType.")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    extract_bronze()
