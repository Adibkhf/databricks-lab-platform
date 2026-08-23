# ============================================================
# Scenario 007 - Reproduction d'un chemin RAW non conforme
# ============================================================

# Génère des valeurs uniques afin qu'Auto Loader voie toujours
# le fichier comme un nouveau fichier.
$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$date = Get-Date -Format "yyyy-MM-dd"
$batch = "batch_$timestamp"
$orderId = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

$localFile = "scenario_007_bad_path.csv"

# Chemin volontairement incorrect :
# ingestion_dt au lieu de ingestion_date.
$invalidPath = "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_dt=$date/$batch"

# Génère un CSV valide : seul le chemin est volontairement incorrect.
"order_id,customer_id,order_date,status`n$orderId,1001,$date,paid" | Set-Content $localFile

# Crée le répertoire hors contrat.
databricks fs mkdir $invalidPath -p DEV

# Dépose le fichier dans le Volume.
databricks fs cp $localFile "$invalidPath/orders.csv" -p DEV

# Vérifie que le fichier existe.
databricks fs ls $invalidPath -p DEV

Write-Host ""
Write-Host "Scenario 007 reproduced."
Write-Host "order_id    : $orderId"
Write-Host "source path : $invalidPath"
Write-Host ""
Write-Host "Execute src/ingestion/bronze_order.py to test the routing."