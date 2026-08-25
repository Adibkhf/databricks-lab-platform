# ============================================================
# Scenario 006 - Reproduce Schema Evolution
# ============================================================

$ErrorActionPreference = "Stop"

$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$date = Get-Date -Format "yyyy-MM-dd"
$batch = "batch_$timestamp"

$orderId1 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$orderId2 = $orderId1 + 1

$localFile = "scenario_006_schema_evolution_$timestamp.csv"
$targetPath = "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=$date/$batch"

# Generate a valid orders CSV with the new channel column.
"order_id,customer_id,order_date,status,channel`n$orderId1,1001,$date,paid,mobile`n$orderId2,1002,$date,pending,web" | Set-Content -Path $localFile -Encoding UTF8

# Create the RAW batch path.
databricks fs mkdir $targetPath -p DEV

# Upload the new file.
databricks fs cp $localFile "$targetPath/orders.csv" -p DEV

# Verify the uploaded file.
databricks fs ls $targetPath -p DEV

Write-Host ""
Write-Host "Scenario 006 file created."
Write-Host "Order ID 1 : $orderId1"
Write-Host "Order ID 2 : $orderId2"
Write-Host "Batch      : $batch"
Write-Host "Path       : $targetPath"
Write-Host ""
Write-Host "Next step:"
Write-Host "Run src/ingestion/bronze_order.py on dbx-lab-dev-single."
