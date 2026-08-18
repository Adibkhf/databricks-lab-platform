## Silver — Application d'un flux CDC INSERT / UPDATE / DELETE

from delta.tables import DeltaTable  # Importe l'API DeltaTable permettant d'exécuter un MERGE sur une table Delta.
from pyspark.sql import functions as F  # Importe les fonctions PySpark utilisées pour préparer le flux CDC.
from pyspark.sql.types import LongType, StringType, StructField, StructType  # Importe les types nécessaires pour définir explicitement le schéma du CDC.

TARGET_TABLE = "dbx_lab_dev.silver.orders_current"  # Définit la table Silver contenant l'état courant des commandes.

cdc_schema = StructType([  # Commence la définition explicite du schéma du flux de changements.
    StructField("operation", StringType(), False),  # Définit le type d'opération CDC : INSERT, UPDATE ou DELETE.
    StructField("order_id", LongType(), False),  # Définit la clé métier permettant de retrouver la commande cible.
    StructField("customer_id", LongType(), True),  # Définit le client concerné, facultatif pour un DELETE.
    StructField("order_date", StringType(), True),  # Définit la date de commande sous forme de chaîne avant conversion.
    StructField("status", StringType(), True),  # Définit le nouveau statut de la commande, facultatif pour un DELETE.
])  # Termine la définition du schéma CDC.

cdc_rows = [  # Crée un petit batch CDC synthétique contenant les trois types de changements.
    ("UPDATE", 50001, 12, "2026-04-01", "cancelled"),  # Simule une modification de la commande 50001.
    ("DELETE", 1, None, None, None),  # Simule la suppression de la commande 1.
    ("INSERT", 60001, 123, "2026-04-02", "created"),  # Simule l'arrivée d'une nouvelle commande 60001.
]  # Termine la définition des événements CDC.

cdc_df = spark.createDataFrame(cdc_rows, schema=cdc_schema)  # Transforme les changements synthétiques en DataFrame Spark.

cdc_df = (  # Commence l'enrichissement technique du flux CDC.
    cdc_df  # Utilise le DataFrame CDC comme point de départ.
    .withColumn("order_date", F.to_date(F.col("order_date")))  # Convertit order_date en véritable type DATE.
    .withColumn("ingestion_date", F.current_date())  # Ajoute la date à laquelle le changement est traité.
    .withColumn("_ingestion_timestamp", F.current_timestamp())  # Ajoute le timestamp précis du traitement CDC.
    .withColumn("_source_file", F.lit("synthetic_cdc_batch_001"))  # Ajoute un identifiant permettant de tracer l'origine du changement.
    .withColumn("_rescued_data", F.lit(None).cast("string"))  # Ajoute la colonne technique attendue par notre schéma Silver.
)  # Termine la préparation du flux CDC.

target = DeltaTable.forName(spark, TARGET_TABLE)  # Charge la table orders_current comme table Delta cible.

merge_operation = (  # Commence la définition du MERGE CDC.
    target.alias("t")  # Donne l'alias t à la table cible.
    .merge(cdc_df.alias("s"), "t.order_id = s.order_id")  # Recherche la commande cible correspondant à chaque changement CDC.
    .whenMatchedDelete(condition="s.operation = 'DELETE'")  # Supprime la commande lorsque le changement reçu est un DELETE.
    .whenMatchedUpdate(  # Définit le comportement lorsqu'un UPDATE correspond à une commande existante.
        condition="s.operation = 'UPDATE'",  # Limite cette action uniquement aux événements UPDATE.
        set={  # Définit les nouvelles valeurs à appliquer à la ligne cible.
            "customer_id": "s.customer_id",  # Met à jour le client avec la valeur reçue dans le changement.
            "order_date": "s.order_date",  # Met à jour la date de commande.
            "status": "s.status",  # Met à jour le statut de la commande.
            "ingestion_date": "s.ingestion_date",  # Mémorise la date de traitement du changement.
            "_ingestion_timestamp": "s._ingestion_timestamp",  # Mémorise le timestamp du changement appliqué.
            "_source_file": "s._source_file",  # Mémorise l'origine synthétique du changement.
            "_rescued_data": "s._rescued_data",  # Maintient la colonne technique de rescued data.
        },  # Termine la liste des colonnes mises à jour.
    )  # Termine la clause UPDATE.
    .whenNotMatchedInsert(  # Définit le comportement lorsqu'un nouvel order_id n'existe pas dans la cible.
        condition="s.operation = 'INSERT'",  # Autorise l'insertion uniquement pour les événements INSERT.
        values={  # Définit les valeurs de la nouvelle commande.
            "order_id": "s.order_id",  # Insère l'identifiant de la nouvelle commande.
            "customer_id": "s.customer_id",  # Insère l'identifiant du client.
            "order_date": "s.order_date",  # Insère la date de commande.
            "status": "s.status",  # Insère le statut initial.
            "ingestion_date": "s.ingestion_date",  # Enregistre la date de traitement.
            "_ingestion_timestamp": "s._ingestion_timestamp",  # Enregistre le timestamp d'ingestion.
            "_source_file": "s._source_file",  # Enregistre l'origine du changement.
            "_rescued_data": "s._rescued_data",  # Initialise la colonne technique rescued data.
        },  # Termine la liste des valeurs insérées.
    )  # Termine la clause INSERT.
)  # Termine la définition complète du MERGE CDC.

merge_operation.execute()  # Applique réellement les INSERT, UPDATE et DELETE sur orders_current.

print("CDC batch applied.")  # Confirme que le batch CDC a été traité.