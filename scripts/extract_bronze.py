# Save as: scripts/extract_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

jdbc_url = "jdbc:postgresql://de-postgres:5432/metastore"
props = {"user": "e5081", "password": "postgres", "driver": "org.postgresql.Driver"}

print("EXTRACTING: Postgres -> Bronze")
df = spark.read.jdbc(url=jdbc_url, table="public.inventory_source", properties=props)
df = df.withColumn("ingestion_timestamp", current_timestamp()) \
       .withColumn("source_file_system", lit("postgres_inventory_db"))

df.write.format("delta").mode("overwrite").save("/home/jovyan/data/bronze/inventory")
print("SUCCESS: Bronze Layer Loaded.")
