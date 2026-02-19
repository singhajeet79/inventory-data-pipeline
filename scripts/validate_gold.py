from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("GoldValidation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

gold_path = "/home/jovyan/data/gold/inventory_summary"

print(f"Validating Gold Table at {gold_path}...")
df = spark.read.format("delta").load(gold_path)

# Rule 1: Table must not be empty
row_count = df.count()
if row_count == 0:
    print("❌ Validation Failed: Gold table is empty!")
    sys.exit(1)

# Rule 2: No NULLs in critical columns
null_categories = df.filter(df.category.isNull()).count()
if null_categories > 0:
    print(f"❌ Validation Failed: Found {null_categories} null categories!")
    sys.exit(1)

# Rule 3: Business Logic - Inventory value must be positive
negative_values = df.filter(df.total_inventory_value < 0).count()
if negative_values > 0:
    print("❌ Validation Failed: Negative inventory values detected!")
    sys.exit(1)

print(f"✅ Validation Passed! {row_count} rows verified.")
spark.stop()
