from pyspark.sql import functions as F  # Importe les fonctions PySpark nécessaires aux transformations.
from pyspark.sql.window import Window  # Importe Window pour classer les différentes versions d'une même commande.

SOURCE_TABLE = "dbx_lab_dev.bronze.orders_raw"  # Définit la table Bronze utilisée comme source.
TARGET_TABLE = "dbx_lab_dev.silver.orders_clean"  # Définit la table Silver contenant les commandes propres et dédupliquées.
QUARANTINE_TABLE = "dbx_lab_dev.silver.orders_quarantine"  # Définit la table Silver contenant les lignes rejetées.

bronze_df = spark.table(SOURCE_TABLE)  # Charge toutes les données Bronze dans un DataFrame Spark.

normalized_df = (  # Commence la normalisation technique des données.
    bronze_df  # Utilise les données Bronze comme point de départ.
    .withColumn("order_id", F.col("order_id").cast("long"))  # Convertit order_id en LONG.
    .withColumn("customer_id", F.col("customer_id").cast("long"))  # Convertit customer_id en LONG.
    .withColumn("order_date", F.to_date(F.col("order_date")))  # Convertit order_date au type DATE.
    .withColumn("status", F.lower(F.trim(F.col("status"))))  # Nettoie et normalise le statut en minuscules.
    .withColumn("ingestion_date", F.to_date(F.col("ingestion_date")))  # Convertit la date d'ingestion au type DATE.
)  # Termine la normalisation.

valid_df = (  # Commence la sélection des lignes techniquement valides.
    normalized_df  # Utilise les données normalisées.
    .filter(F.col("order_id").isNotNull())  # Conserve uniquement les lignes avec un order_id exploitable.
    .filter(F.col("customer_id").isNotNull())  # Conserve uniquement les lignes avec un customer_id exploitable.
    .filter(F.col("order_date").isNotNull())  # Conserve uniquement les dates de commande valides.
    .filter(F.col("status").isin("created", "paid", "shipped", "cancelled"))  # Conserve uniquement les statuts autorisés.
    .filter(F.col("_rescued_data").isNull())  # Exclut les lignes ayant des données incompatibles récupérées par Auto Loader.
)  # Termine la sélection des lignes valides.

invalid_df = (  # Commence la sélection des lignes à mettre en quarantaine.
    normalized_df  # Utilise les données normalisées.
    .filter(  # Applique les règles permettant d'identifier une ligne invalide.
        F.col("order_id").isNull()  # Détecte un order_id invalide ou absent.
        | F.col("customer_id").isNull()  # Détecte un customer_id invalide ou absent.
        | F.col("order_date").isNull()  # Détecte une date invalide.
        | (~F.col("status").isin("created", "paid", "shipped", "cancelled"))  # Détecte un statut non autorisé.
        | F.col("_rescued_data").isNotNull()  # Détecte une ligne ayant déclenché rescued data.
    )  # Termine la condition de quarantaine.
)  # Termine la construction du DataFrame de quarantaine.

dedup_window = Window.partitionBy("order_id").orderBy(F.col("_ingestion_timestamp").desc(), F.col("_source_file").desc())  # Classe les versions de chaque order_id de la plus récente à la plus ancienne.

deduplicated_df = (  # Commence la déduplication des commandes valides.
    valid_df  # Utilise uniquement les commandes ayant passé les contrôles de qualité.
    .withColumn("_row_number", F.row_number().over(dedup_window))  # Attribue le rang 1 à la version la plus récente de chaque order_id.
    .filter(F.col("_row_number") == 1)  # Ne conserve que la version la plus récente de chaque commande.
    .drop("_row_number")  # Supprime la colonne technique utilisée pour effectuer la déduplication.
)  # Termine la déduplication.

deduplicated_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)  # Réécrit la table Silver avec les commandes propres et uniques.

invalid_df.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TABLE)  # Réécrit la table de quarantaine avec les lignes invalides.

print(f"Valid before deduplication: {valid_df.count()}")  # Affiche le nombre de lignes valides avant suppression des doublons.
print(f"Silver after deduplication: {deduplicated_df.count()}")  # Affiche le nombre final de commandes uniques.
print(f"Quarantined rows: {invalid_df.count()}")  # Affiche le nombre de lignes invalides isolées.