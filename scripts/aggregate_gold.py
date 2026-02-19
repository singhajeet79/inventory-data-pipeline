from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, current_date
import sys

# 1. Initialize Spark with low-memory footprint configs
spark = SparkSession.builder \
    .appName("GoldAggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.driver.memory", "512m") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

try:
    silver_path = "/home/jovyan/data/silver/inventory_managed"
    gold_path = "/home/jovyan/data/gold/inventory_summary"

    print("AGGREGATING: Silver -> Gold...")
    df = spark.read.format("delta").load(silver_path)

    # 2. Business Logic Aggregation
    gold_df = df.groupBy("category").agg(
        sum("item_id").alias("total_items"), # Using sum for count logic or count("item_id")
        sum("base_price").alias("total_inventory_value"),
        avg("base_price").alias("average_item_cost")
    ).withColumn("report_date", current_date())

    # 3. Write to Gold (Overwrite mode for the summary table)
    gold_df.write.format("delta").mode("overwrite").save(gold_path)
    
    print("✅ Gold Layer Success")

except Exception as e:
    print(f"❌ Gold Layer Failed: {e}")
    sys.exit(1)

finally:
    # 4. The Graceful Exit
    # Stopping the session explicitly prevents the "Zombie" Spark process 
    # that often leads to Airflow timeout/failure status.
    spark.stop()
    sys.exit(0)
