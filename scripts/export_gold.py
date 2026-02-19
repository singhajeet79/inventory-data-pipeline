from pyspark.sql import SparkSession
import os
import glob
import shutil

spark = SparkSession.builder.appName("GoldExport").getOrCreate()

gold_path = "/home/jovyan/data/gold/inventory_summary"
export_dir = "/home/jovyan/data/exports"
final_file = f"{export_dir}/inventory_report.csv"

# 1. Read the Gold Data
df = spark.read.format("delta").load(gold_path)

# 2. Write as a single CSV to a temp folder
# coalesce(1) forces everything into one file
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{export_dir}/temp_export")

# 3. Use standard Python to rename the spark-generated part file to a clean name
temp_path = f"{export_dir}/temp_export"
part_file = glob.glob(f"{temp_path}/part-*.csv")[0]

if not os.path.exists(export_dir):
    os.makedirs(export_dir)

shutil.copy(part_file, final_file)
shutil.rmtree(temp_path)

print(f"✅ Export Successful: {final_file}")
spark.stop()
