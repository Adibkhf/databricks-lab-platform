from pyspark.sql import functions as F  # Importe les fonctions PySpark nécessaires aux transformations.

SOURCE_TABLE = "dbx_lab_dev.bronze.orders_raw"  # Définit la table Bronze utilisée comme source.
TARGET_TABLE = "dbx_lab_dev.silver.orders_clean"  # Définit la table Silver qui contiendra les lignes valides.
QUARANTINE_TABLE = "dbx_lab_dev.silver.orders_quarantine"  # Définit la table qui conservera les lignes rejetées.

bronze_df = spark.table(SOURCE_TABLE)  # Charge la table Bronze orders_raw dans un DataFrame Spark.

normalized_df = (  # Commence la normalisation des données Bronze.
    bronze_df  # Utilise le DataFrame Bronze comme point de départ.
    .withColumn("order_id", F.col("order_id").cast("long"))  # Force order_id au type LONG.
    .withColumn("customer_id", F.col("customer_id").cast("long"))  # Force customer_id au type LONG.
    .withColumn("order_date", F.to_date(F.col("order_date")))  # Convertit order_date en véritable type DATE.
    .withColumn("status", F.lower(F.trim(F.col("status"))))  # Supprime les espaces et normalise status en minuscules.
    .withColumn("ingestion_date", F.to_date(F.col("ingestion_date")))  # Convertit ingestion_date en véritable type DATE.
)  # Termine la normalisation des colonnes.

valid_df = (  # Commence la sélection des lignes considérées comme valides.
    normalized_df  # Utilise les données précédemment normalisées.
    .filter(F.col("order_id").isNotNull())  # Rejette toute ligne sans identifiant de commande exploitable.
    .filter(F.col("customer_id").isNotNull())  # Rejette toute ligne sans identifiant client exploitable.
    .filter(F.col("order_date").isNotNull())  # Rejette toute ligne dont la date n'est pas interprétable.
    .filter(F.col("status").isin("created", "paid", "shipped", "cancelled"))  # Ne conserve que les statuts autorisés.
    .filter(F.col("_rescued_data").isNull())  # Écarte les lignes contenant des données récupérées par Auto Loader.
)  # Termine la définition du dataset Silver valide.

invalid_df = (  # Commence la construction du dataset de quarantaine.
    normalized_df  # Repart des données normalisées.
    .filter(  # Conserve toute ligne qui viole au moins une règle de qualité.
        F.col("order_id").isNull()  # Identifie les lignes dont order_id est invalide ou absent.
        | F.col("customer_id").isNull()  # Identifie les lignes dont customer_id est invalide ou absent.
        | F.col("order_date").isNull()  # Identifie les lignes dont order_date est invalide.
        | (~F.col("status").isin("created", "paid", "shipped", "cancelled"))  # Identifie les statuts non autorisés.
        | F.col("_rescued_data").isNotNull()  # Identifie les lignes ayant déclenché rescued data.
    )  # Termine la condition de quarantaine.
)  # Termine la définition du dataset invalide.

valid_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)  # Écrit les lignes valides dans une table Delta Silver managée.

invalid_df.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TABLE)  # Écrit séparément les lignes invalides pour investigation.

print(f"Silver valid rows: {valid_df.count()}")  # Affiche le nombre de commandes ayant passé les contrôles.
print(f"Quarantined rows: {invalid_df.count()}")  # Affiche le nombre de commandes rejetées.