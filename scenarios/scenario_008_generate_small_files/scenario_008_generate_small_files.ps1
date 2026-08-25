# ============================================================
# Scenario 008 - Generate 200 small RAW files
# ============================================================

$ErrorActionPreference = "Stop"

$date = Get-Date -Format "yyyy-MM-dd"
$basePath = "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=$date"

# Identifiant de départ suffisamment élevé pour éviter
# les collisions avec les commandes existantes.
$baseOrderId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

for ($i = 1; $i -le 200; $i++) {

    $batchNumber = 800000 + $i
    $batch = "batch_$batchNumber"

    $orderId = $baseOrderId + $i
    $customerId = 1000 + ($i % 100)

    $localFile = "scenario_008_$batch.csv"
    $targetPath = "$basePath/$batch"

    # Chaque fichier contient volontairement une seule ligne.
    # Le schéma inclut channel pour rester compatible
    # avec le schéma actuel de orders.
    "order_id,customer_id,order_date,status,channel`n$orderId,$customerId,$date,paid,web" | Set-Content -Path $localFile -Encoding UTF8

    databricks fs mkdir $targetPath -p DEV
    databricks fs cp $localFile "$targetPath/orders.csv" -p DEV

    # Nettoyage du fichier temporaire local.
    Remove-Item $localFile

    Write-Host "[$i/200] $batch/orders.csv"
}

Write-Host ""
Write-Host "200 small files uploaded."