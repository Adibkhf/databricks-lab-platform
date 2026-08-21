from pyspark.sql import functions as F  # Importe les fonctions Spark nécessaires aux agrégations.

SOURCE_TABLE = "dbx_lab_dev.silver.orders_current"  # Définit la table Silver contenant l'état courant des commandes.
TARGET_TABLE = "dbx_lab_dev.gold.orders_daily"  # Définit la table Gold analytique produite par le job.

orders = spark.table(SOURCE_TABLE)  # Lit la table Silver depuis Unity Catalog.

orders_daily = (  # Construit l'agrégation Gold.
    orders  # Utilise les commandes Silver nettoyées et consolidées.
    .groupBy("order_date", "status")  # Regroupe les commandes par jour et statut.
    .agg(F.count("*").alias("order_count"))  # Compte le nombre de commandes de chaque groupe.
)

(  # Commence l'écriture de la table Gold.
    orders_daily.write  # Utilise l'API batch DataFrameWriter.
    .format("delta")  # Stocke la table au format Delta.
    .mode("overwrite")  # Reconstruit complètement cette petite table agrégée à chaque run.
    .option("overwriteSchema", "true")  # Permet d'aligner le schéma de destination avec le DataFrame.
    .saveAsTable(TARGET_TABLE)  # Écrit dans Unity Catalog.
)

print("Gold orders_daily completed.")  # Confirme la fin du traitement.