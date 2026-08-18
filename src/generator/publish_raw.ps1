param(
    [string]$Profile = "small",
    [string]$IngestionDate = (Get-Date -Format "yyyy-MM-dd"),
    [string]$BatchId = "batch_001"
)

$Bucket = "gs://databricks-lab-dev-raw-1966"
$SourceRoot = "data/$Profile"

$Tables = @(
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "transactions",
    "events"
)

foreach ($Table in $Tables) {

    $Source = "$SourceRoot/$Table.csv"

    $Destination = `
        "$Bucket/ecommerce/$Table/ingestion_date=$IngestionDate/$BatchId/$Table.csv"

    Write-Host "$Table -> $Destination"

    gcloud storage cp $Source $Destination

    if ($LASTEXITCODE -ne 0) {
        throw "Upload failed for $Table"
    }
}

Write-Host "RAW publication completed."