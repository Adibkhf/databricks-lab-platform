# ============================================================
# Scenario 001 - Auto Loader incremental ingestion
# ============================================================

$batch = "batch_004"
$date = "2026-08-23"

$localFile = "scenario_001_batch_004.csv"
$volumePath = "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=$date/$batch"

# Crée un petit nouveau lot RAW.
@"
order_id,customer_id,order_date,status
900001,1001,2026-08-23,paid
900002,1002,2026-08-23,pending
"@ | Set-Content $localFile

# Crée le dossier du nouveau batch dans le Volume Unity Catalog.
databricks fs mkdir $volumePath -p DEV

# Dépose le nouveau fichier.
databricks fs cp $localFile "$volumePath/orders.csv" -p DEV

# Vérifie sa présence.
databricks fs ls $volumePath -p DEV

# Lance la pipeline Databricks gérée par Asset Bundles.
databricks bundle run -t dev ecommerce_dev_pipeline