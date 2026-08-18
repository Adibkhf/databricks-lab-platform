from delta.tables import DeltaTable  # Importe l'API DeltaTable nécessaire pour exécuter un MERGE Delta Lake.

SOURCE_TABLE = "dbx_lab_dev.silver.orders_clean"  # Définit la table Silver dédupliquée utilisée comme source.
TARGET_TABLE = "dbx_lab_dev.silver.orders_current"  # Définit la table cible contenant l'état courant des commandes.

source_df = spark.table(SOURCE_TABLE)  # Charge les commandes propres et dédupliquées depuis Silver.

if not spark.catalog.tableExists(TARGET_TABLE):  # Vérifie si la table cible existe déjà dans Unity Catalog.
    source_df.limit(0).write.format("delta").saveAsTable(TARGET_TABLE)  # Crée une table Delta vide avec exactement le même schéma que la source.

target = DeltaTable.forName(spark, TARGET_TABLE)  # Récupère la table cible sous forme de DeltaTable pour pouvoir effectuer le MERGE.

merge_operation = (  # Commence la définition du MERGE idempotent.
    target.alias("t")  # Donne l'alias t à la table cible.
    .merge(source_df.alias("s"), "t.order_id = s.order_id")  # Compare la source et la cible grâce à la clé métier order_id.
    .whenMatchedUpdateAll(condition="s._ingestion_timestamp > t._ingestion_timestamp")  # Met à jour uniquement si la version source est réellement plus récente.
    .whenNotMatchedInsertAll()  # Insère une commande uniquement si son order_id n'existe pas encore dans la cible.
)  # Termine la définition du MERGE.

merge_operation.execute()  # Exécute réellement l'opération MERGE sur la table Delta.

print(f"Source rows: {source_df.count()}")  # Affiche le nombre de commandes présentes dans la source Silver.
print(f"Target rows: {spark.table(TARGET_TABLE).count()}")  # Affiche le nombre de commandes présentes après le MERGE.