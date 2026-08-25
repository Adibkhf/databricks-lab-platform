from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

# Racine RAW surveillée par Auto Loader.
SOURCE_PATH = "/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders"

# Auto Loader conserve ici les différentes versions du schéma détecté.
SCHEMA_PATH = "/Volumes/dbx_lab_dev/landing/raw/_schemas/bronze_orders"

# Le checkpoint mémorise la progression du stream.
CHECKPOINT_PATH = "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/bronze_orders"

# Table Bronze principale.
TARGET_TABLE = "dbx_lab_dev.bronze.orders_raw"

# Table contenant les fichiers hors contrat.
QUARANTINE_TABLE = "dbx_lab_dev.bronze.orders_quarantine"

# Contrat attendu :
# .../ingestion_date=YYYY-MM-DD/batch_XXX/orders.csv
VALID_PATH_PATTERN = (
    r".*/ingestion_date=\d{4}-\d{2}-\d{2}/batch_\d+/orders\.csv$"
)

# Identifiants de transaction Delta.
#
# Chaque destination possède son propre txnAppId.
# batch_id sera utilisé comme txnVersion.
#
# Si un micro-batch est rejoué après un échec,
# Delta reconnaît que cette transaction a déjà été appliquée
# et évite de réécrire les mêmes données.
TARGET_TXN_APP_ID = "bronze-orders-raw-v1"
QUARANTINE_TXN_APP_ID = "bronze-orders-quarantine-v1"


# ============================================================
# AUTO LOADER
# ============================================================

df = (
    spark.readStream
    .format("cloudFiles")

    # Les fichiers RAW orders sont des CSV.
    .option("cloudFiles.format", "csv")

    # Emplacement où Auto Loader conserve son schéma.
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)

    # Autorise l'apparition de nouvelles colonnes.
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")

    # channel est maintenant une colonne explicitement connue
    # et son type attendu est STRING.
    .option("cloudFiles.schemaHints", "channel STRING")

    # Permet d'inférer INT, DATE, STRING... au lieu de tout lire en STRING.
    .option("cloudFiles.inferColumnTypes", "true")

    # Première ligne du CSV = noms des colonnes.
    .option("header", "true")

    # Racine surveillée.
    .load(SOURCE_PATH)

    # Timestamp technique de réception dans Bronze.
    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )

    # Chemin du fichier source utilisé pour la traçabilité
    # et pour contrôler le contrat de landing.
    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )
)


# ============================================================
# DEBUG SCHEMA
# ============================================================

print("Schema loaded by Auto Loader:")
df.printSchema()


# ============================================================
# TRAITEMENT DE CHAQUE MICRO-BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    print(f"Processing micro-batch: {batch_id}")

    # Le DataFrame est utilisé deux fois :
    # une fois pour Bronze et une fois pour Quarantine.
    # On évite donc de recalculer inutilement le micro-batch.
    batch_df.persist()

    try:

        # ====================================================
        # 1. FICHIERS CONFORMES
        # ====================================================

        valid_df = (
            batch_df
            .filter(
                F.col("_source_file").rlike(VALID_PATH_PATTERN)
            )
        )

        # Écriture Bronze principale.
        #
        # mergeSchema :
        # ajoute automatiquement les nouvelles colonnes
        # comme "channel" dans la table Delta.
        #
        # txnAppId + txnVersion :
        # rendent cette écriture idempotente lors d'un retry
        # du même micro-batch.
        (
            valid_df.write
            .format("delta")
            .option("mergeSchema", "true")
            .option("txnAppId", TARGET_TXN_APP_ID)
            .option("txnVersion", str(batch_id))
            .mode("append")
            .saveAsTable(TARGET_TABLE)
        )


        # ====================================================
        # 2. FICHIERS NON CONFORMES
        # ====================================================

        invalid_df = (
            batch_df 
            .filter(
                ~F.col("_source_file").rlike(VALID_PATH_PATTERN)
            )
            .withColumn(
                "_quarantine_reason",
                F.lit("INVALID_SOURCE_PATH")
            )
        )

        # Même stratégie d'évolution de schéma pour Quarantine.
        #
        # Si une nouvelle colonne comme "channel" apparaît,
        # la Quarantine doit également pouvoir la conserver.
        #
        # Elle possède un txnAppId différent de orders_raw,
        # car il s'agit d'une transaction Delta distincte.
        (
            invalid_df.write
            .format("delta")
            .option("mergeSchema", "true")
            .option("txnAppId", QUARANTINE_TXN_APP_ID)
            .option("txnVersion", str(batch_id))
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
        )

        print(f"Micro-batch {batch_id} processed successfully.")

    finally:

        # Libère la mémoire après traitement du micro-batch,
        # y compris si une écriture échoue.
        batch_df.unpersist()


# ============================================================
# STREAMING QUERY
# ============================================================

query = (
    df.writeStream

    # Permet de router un même micro-batch
    # vers plusieurs destinations Delta.
    .foreachBatch(process_batch)

    # Conserve le checkpoint existant.
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    # Traite tous les fichiers actuellement en attente
    # puis termine proprement.
    .trigger(availableNow=True)

    .start()
)


# Attend la fin du traitement AvailableNow.
query.awaitTermination()

print("Bronze ingestion completed.")