import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DateType,
    StringType,
)

# ============================================================
# CONFIGURATION DU SCENARIO
# ============================================================

# On limite volontairement la lecture à la date du test.
SOURCE_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/"
    "ingestion_date=2026-08-25"
)

# Etat Auto Loader dédié au scénario.
# Aucun impact sur le checkpoint de la vraie pipeline.
SCHEMA_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_schemas/"
    "scenario_008_small_files"
)

CHECKPOINT_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/"
    "scenario_008_small_files"
)

# Table dédiée au benchmark.
TARGET_TABLE = "dbx_lab_dev.bronze.scenario_008_small_files"

# Les 200 fichiers créés précédemment sont :
# batch_800001 ... batch_800200
SCENARIO_PATH_PATTERN = r".*/batch_8\d{5}/orders\.csv$"


# ============================================================
# SCHEMA EXPLICITE
# ============================================================
# On ne veut pas mesurer l'inférence de schéma ici.
# On veut principalement mesurer l'overhead des petits fichiers.

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
])


# ============================================================
# AUTO LOADER
# ============================================================

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("header", "true")
    .schema(orders_schema)
    .load(SOURCE_PATH)

    # Conserve le fichier d'origine pour compter les fichiers traités.
    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )

    # Garde uniquement les fichiers générés pour Scenario 008.
    .filter(
        F.col("_source_file").rlike(SCENARIO_PATH_PATTERN)
    )
)


# ============================================================
# BENCHMARK
# ============================================================

start_time = time.perf_counter()

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()

elapsed_seconds = time.perf_counter() - start_time


# ============================================================
# RESULTATS
# ============================================================

result_df = spark.table(TARGET_TABLE)

total_rows = result_df.count()

source_files = (
    result_df
    .select("_source_file")
    .distinct()
    .count()
)

print("========================================")
print("SCENARIO 008 - SMALL FILES BENCHMARK")
print("========================================")
print(f"ROWS={total_rows}")
print(f"SOURCE_FILES={source_files}")
print(f"INGESTION_DURATION_SECONDS={elapsed_seconds:.2f}")
print("========================================")