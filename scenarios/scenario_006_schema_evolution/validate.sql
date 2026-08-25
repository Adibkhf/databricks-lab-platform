-- ============================================================
-- Scenario 006 - Validation
-- ============================================================

-- 1. Verify that channel exists in the main Bronze table.
DESCRIBE TABLE dbx_lab_dev.bronze.orders_raw;

-- 2. Verify that channel also exists in the quarantine table.
DESCRIBE TABLE dbx_lab_dev.bronze.orders_quarantine;

-- 3. Inspect rows that contain the evolved column.
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    channel,
    _source_file,
    _ingestion_timestamp
FROM dbx_lab_dev.bronze.orders_raw
WHERE channel IS NOT NULL
ORDER BY _ingestion_timestamp DESC;

-- 4. Detect duplicates potentially caused by retries.
SELECT
    order_id,
    COUNT(*) AS row_count
FROM dbx_lab_dev.bronze.orders_raw
WHERE channel IS NOT NULL
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 5. Inspect quarantined rows that also carry channel.
SELECT
    order_id,
    channel,
    _source_file,
    _quarantine_reason
FROM dbx_lab_dev.bronze.orders_quarantine
WHERE channel IS NOT NULL
ORDER BY _ingestion_timestamp DESC;
