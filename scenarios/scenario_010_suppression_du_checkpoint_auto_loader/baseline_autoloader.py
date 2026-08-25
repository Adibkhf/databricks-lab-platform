# ============================================================
# Scenario 010 - Baseline Auto Loader
# ============================================================

import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DateType, StringType


SOURCE_PATH = "/Volumes/dbx_lab_dev/landing/raw/scenarios/scenario_010/orders"

CHECKPOINT_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/"
    "scenario_010_orders"
)

TARGET_TABLE = "dbx_lab_dev.bronze.scenario_010_orders_raw"


# Schéma métier attendu des fichiers orders.
schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
])


df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema)
    .load(SOURCE_PATH)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)


start_time = time.perf_counter()


query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()


elapsed = time.perf_counter() - start_time


result = spark.table(TARGET_TABLE)

print("========================================")
print("SCENARIO 010 - BASELINE")
print("========================================")
print(f"ROWS={result.count()}")
print(f"SOURCE_FILES={result.select('_source_file').distinct().count()}")
print(f"INGESTION_DURATION_SECONDS={elapsed:.2f}")
print("========================================")