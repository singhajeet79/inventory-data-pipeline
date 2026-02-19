# Save as: scripts/transform_silver.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, round, current_timestamp
from delta.tables import DeltaTable
import os

# Initialize Spark with Delta Support
spark = SparkSession.builder \
    .appName("SilverTransform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

bronze_path = "/home/jovyan/data/bronze/inventory"
silver_path = "/home/jovyan/data/silver/inventory_managed"

print(f"Reading from {bronze_path}...")
bronze_df = spark.read.format("delta").load(bronze_path)

# Logic to handle potential column name variation from Bronze
cols = bronze_df.columns
source_price_col = "price" if "price" in cols else "base_price"

# Apply Transformations
silver_df = bronze_df.select(
    "item_id",
    "item_name",
    upper(col("category")).alias("category"),
    round(col(source_price_col), 2).alias("base_price")
).withColumn("last_updated", current_timestamp())

# Upsert (Merge) Logic
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("Creating Silver Table...")
    silver_df.write.format("delta").save(silver_path)
else:
    print("Merging into Silver Table...")
    dt = DeltaTable.forPath(spark, silver_path)
    
    # Indentation fix: ensure the merge chain is aligned inside the 'else' block
    dt.alias("t").merge(
        silver_df.alias("s"),
        "t.item_id = s.item_id"
    ).whenMatchedUpdate(set = {
        "item_name": "s.item_name",
        "category": "s.category",
        "base_price": "s.base_price",
        "last_updated": "s.last_updated"
    }).whenNotMatchedInsertAll().execute()

print("✅ Silver Layer Success")
