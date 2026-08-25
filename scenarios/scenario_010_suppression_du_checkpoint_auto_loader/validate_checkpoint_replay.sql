-- ============================================================
-- Scenario 010 - Auto Loader Checkpoint Loss
-- SQL validation checks
-- ============================================================

-- 1. BASELINE BRONZE DE REFERENCE
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows,
    COUNT(DISTINCT _source_file) AS source_files
FROM dbx_lab_dev.bronze.orders_raw;

-- 2. CONTROLE DE LA TABLE BRONZE ISOLEE
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows,
    COUNT(DISTINCT _source_file) AS source_files
FROM dbx_lab_dev.bronze.scenario_010_orders_raw;

-- 3. MESURE DES DOUBLONS APRES PERTE DU CHECKPOINT
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows
FROM dbx_lab_dev.bronze.scenario_010_orders_raw;

-- 4. IDENTIFIER LES ORDER_ID LES PLUS DUPLIQUES
SELECT
    order_id,
    COUNT(*) AS row_count
FROM dbx_lab_dev.bronze.scenario_010_orders_raw
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC, order_id;

-- 5. VERIFIER LES CLES METIER NULL EN BRONZE
SELECT
    COUNT(*) AS null_order_ids
FROM dbx_lab_dev.bronze.scenario_010_orders_raw
WHERE order_id IS NULL;

-- 6. RECONSTRUIRE UNE SILVER IDEMPOTENTE
CREATE OR REPLACE TABLE dbx_lab_dev.silver.scenario_010_orders_current AS
SELECT * EXCEPT(rn)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _ingestion_timestamp DESC, _source_file DESC
        ) AS rn
    FROM dbx_lab_dev.bronze.scenario_010_orders_raw
    WHERE order_id IS NOT NULL
)
WHERE rn = 1;

-- 7. VALIDATION SILVER
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids
FROM dbx_lab_dev.silver.scenario_010_orders_current;

-- 8. CONTROLE DETAILLE DES NULLS SILVER
SELECT
    COUNT(*) AS null_order_ids
FROM dbx_lab_dev.silver.scenario_010_orders_current
WHERE order_id IS NULL;

-- 9. CONTROLE DE NON-REGRESSION METIER
-- Attendu :
-- total_rows      = 50006
-- distinct_orders = 50006
-- duplicate_rows  = 0
-- null_order_ids  = 0
SELECT
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT order_id)
         AND SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS silver_idempotence_check
FROM dbx_lab_dev.silver.scenario_010_orders_current;
