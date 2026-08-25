$ErrorActionPreference = "Stop"

$root = "dbfs:/Volumes/dbx_lab_dev/landing/raw/scenarios/scenario_010/orders"

databricks fs mkdir "$root/ingestion_date=2026-08-16/batch_001" -p DEV
databricks fs mkdir "$root/ingestion_date=2026-08-17/batch_002" -p DEV
databricks fs mkdir "$root/ingestion_date=2026-08-17/batch_003" -p DEV
databricks fs mkdir "$root/ingestion_date=2026-08-23/batch_004" -p DEV
databricks fs mkdir "$root/ingestion_date=2026-08-25/batch_007" -p DEV
databricks fs mkdir "$root/ingestion_dt=2026-08-23/batch_005" -p DEV

databricks fs cp "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=2026-08-16/batch_001/orders.csv" "$root/ingestion_date=2026-08-16/batch_001/orders.csv" -p DEV
databricks fs cp "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=2026-08-17/batch_002/orders_bad.csv" "$root/ingestion_date=2026-08-17/batch_002/orders_bad.csv" -p DEV
databricks fs cp "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=2026-08-17/batch_003/orders_duplicate.csv" "$root/ingestion_date=2026-08-17/batch_003/orders_duplicate.csv" -p DEV
databricks fs cp "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=2026-08-23/batch_004/orders.csv" "$root/ingestion_date=2026-08-23/batch_004/orders.csv" -p DEV
databricks fs cp "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_date=2026-08-25/batch_007/orders.csv" "$root/ingestion_date=2026-08-25/batch_007/orders.csv" -p DEV
databricks fs cp "dbfs:/Volumes/dbx_lab_dev/landing/raw/ecommerce/orders/ingestion_dt=2026-08-23/batch_005/orders.csv" "$root/ingestion_dt=2026-08-23/batch_005/orders.csv" -p DEV

Write-Host "Scenario 010 RAW prepared successfully."