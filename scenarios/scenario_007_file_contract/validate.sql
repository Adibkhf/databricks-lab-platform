-- ============================================================
-- Scenario 007 - Validation
-- ============================================================

-- Les fichiers hors contrat ne doivent plus entrer dans Bronze.
SELECT
    order_id,
    _source_file
FROM dbx_lab_dev.bronze.orders_raw
WHERE _source_file LIKE '%/ingestion_dt=%';


-- Les fichiers hors contrat doivent être conservés en quarantine.
SELECT
    order_id,
    _source_file,
    _quarantine_reason
FROM dbx_lab_dev.bronze.orders_quarantine
WHERE _source_file LIKE '%/ingestion_dt=%'
ORDER BY _ingestion_timestamp DESC;