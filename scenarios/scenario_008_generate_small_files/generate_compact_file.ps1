# ============================================================
# Scenario 008 - Generate 1 compact file with 200 rows
# ============================================================

$ErrorActionPreference = "Stop"

$date = Get-Date -Format "yyyy-MM-dd"
$batch = "batch_899999"

$targetPath = "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=$date/$batch"
$localFile = "scenario_008_compact.csv"

$baseOrderId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

$rows = @("order_id,customer_id,order_date,status,channel")

for ($i = 1; $i -le 200; $i++) {
    $orderId = $baseOrderId + $i
    $customerId = 1000 + ($i % 100)
    $rows += "$orderId,$customerId,$date,paid,web"
}

$rows | Set-Content -Path $localFile -Encoding UTF8

databricks fs mkdir $targetPath -p DEV
databricks fs cp $localFile "$targetPath/orders.csv" -p DEV

Remove-Item $localFile

Write-Host ""
Write-Host "Compact file uploaded:"
Write-Host "$targetPath/orders.csv"
Write-Host "Rows: 200"