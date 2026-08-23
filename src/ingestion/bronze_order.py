from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

# Racine RAW surveillée par Auto Loader.
SOURCE_PATH = "/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders"

# Auto Loader conserve ici l'évolution du schéma détecté.
SCHEMA_PATH = "/Volumes/dbx_lab_dev/landing/raw/_schemas/bronze_orders"

# Checkpoint principal : mémorise les fichiers déjà traités.
CHECKPOINT_PATH = "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/bronze_orders"

# Fichiers respectant le contrat de landing.
TARGET_TABLE = "dbx_lab_dev.bronze.orders_raw"

# Fichiers/lignes provenant d'un chemin non conforme.
QUARANTINE_TABLE = "dbx_lab_dev.bronze.orders_quarantine"

# Contrat attendu :
# .../ingestion_date=YYYY-MM-DD/batch_XXX/orders.csv
VALID_PATH_PATTERN = r".*/ingestion_date=\d{4}-\d{2}-\d{2}/batch_\d+/orders\.csv$"


# ============================================================
# AUTO LOADER
# ============================================================

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(SOURCE_PATH)

    # Métadonnées nécessaires pour tracer l'origine de chaque ligne.
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)


# ============================================================
# TRAITEMENT DE CHAQUE MICRO-BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    # --------------------------------------------------------
    # 1. Données conformes
    # --------------------------------------------------------

    valid_df = (
        batch_df
        .filter(F.col("_source_file").rlike(VALID_PATH_PATTERN))
    )

    # Les données conformes continuent vers Bronze.
    valid_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)


    # --------------------------------------------------------
    # 2. Données non conformes
    # --------------------------------------------------------

    invalid_df = (
        batch_df
        .filter(~F.col("_source_file").rlike(VALID_PATH_PATTERN))
        .withColumn(
            "_quarantine_reason",
            F.lit("INVALID_SOURCE_PATH")
        )
    )

    # Les données hors contrat sont isolées au lieu d'entrer dans Bronze.
    invalid_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(QUARANTINE_TABLE)

    print(f"Micro-batch {batch_id} processed.")


# ============================================================
# STREAMING QUERY
# ============================================================

query = (
    df.writeStream

    # foreachBatch permet d'envoyer le même micro-batch
    # vers deux destinations différentes.
    .foreachBatch(process_batch)

    # Le checkpoint Auto Loader existant est conservé.
    .option("checkpointLocation", CHECKPOINT_PATH)

    # Traite le backlog actuel puis arrête la query.
    .trigger(availableNow=True)

    .start()
)

query.awaitTermination()

print("Bronze ingestion completed.")