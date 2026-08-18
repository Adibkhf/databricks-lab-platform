from pyspark.sql import functions as F  # Importe les fonctions PySpark utiles pour creer de nouvelles colonnes.

SOURCE_PATH = "/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders"  # Definit le dossier RAW contenant les fichiers orders.
SCHEMA_PATH = "/Volumes/dbx_lab_dev/landing/raw/_schemas/bronze_orders"  # Definit ou Auto Loader conservera le schema decouvert.
CHECKPOINT_PATH = "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/bronze_orders"  # Definit ou Spark memorisera la progression du streaming.
TARGET_TABLE = "dbx_lab_dev.bronze.orders_raw"  # Definit la table Delta Bronze dans laquelle les donnees seront ecrites.

df = (  # Commence la definition du DataFrame streaming.
    spark.readStream  # Demande a Spark de lire les donnees sous forme de streaming.
    .format("cloudFiles")  # Active Auto Loader via la source cloudFiles.
    .option("cloudFiles.format", "csv")  # Indique que les fichiers sources sont au format CSV.
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)  # Indique ou Auto Loader doit stocker son historique de schema.
    .option("cloudFiles.inferColumnTypes", "true")  # Demande a Auto Loader d'inferer les types au lieu de tout lire en STRING.
    .option("header", "true")  # Indique que la premiere ligne de chaque CSV contient les noms de colonnes.
    .load(SOURCE_PATH)  # Lit les fichiers presents sous le repertoire orders.
    .withColumn("_ingestion_timestamp", F.current_timestamp())  # Ajoute l'heure a laquelle chaque ligne est ingeree dans Bronze.
    .withColumn("_source_file", F.col("_metadata.file_path"))  # Ajoute le chemin du fichier source pour assurer la traçabilite.
)  # Termine la definition du DataFrame streaming.

query = (  # Commence la configuration de l'ecriture streaming.
    df.writeStream  # Indique que le DataFrame streaming doit maintenant être ecrit.
    .format("delta")  # Choisit Delta Lake comme format de destination.
    .option("checkpointLocation", CHECKPOINT_PATH)  # Indique ou Spark doit enregistrer la progression de cette query.
    .trigger(availableNow=True)  # Traite tous les fichiers actuellement disponibles puis arrête automatiquement la query.
    .toTable(TARGET_TABLE)  # ecrit les donnees dans la table Unity Catalog dbx_lab_dev.bronze.orders_raw.
)  # Termine la configuration et demarre la query streaming.

query.awaitTermination()  # Attend que le traitement availableNow soit completement termine avant de continuer.
print("Bronze ingestion completed.")  # Affiche un message lorsque l'ingestion est terminee.