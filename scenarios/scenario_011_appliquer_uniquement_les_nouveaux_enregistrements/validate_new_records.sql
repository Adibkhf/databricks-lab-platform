-- ============================================================
-- Scenario 011 - CDC INSERT
-- SQL manipulations and validation checks
-- ============================================================

-- 1. Baseline de la vraie Silver avant le scénario.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    MAX(_ingestion_timestamp) AS latest_ingestion
FROM dbx_lab_dev.silver.orders_current;

-- 2. Créer un flux CDC contenant uniquement des INSERT.
CREATE OR REPLACE TABLE dbx_lab_dev.bronze.scenario_011_orders_cdc AS
SELECT * FROM VALUES
    (960001, 1001, DATE'2026-08-26', 'paid',    1L, 'INSERT'),
    (960002, 1002, DATE'2026-08-26', 'pending', 1L, 'INSERT'),
    (960003, 1003, DATE'2026-08-26', 'paid',    1L, 'INSERT')
AS t(order_id, customer_id, order_date, status, sequence_num, operation);

-- 3. Vérifier le CDC source.
SELECT *
FROM dbx_lab_dev.bronze.scenario_011_orders_cdc
ORDER BY order_id;

-- 4. Créer une cible Silver isolée.
CREATE OR REPLACE TABLE dbx_lab_dev.silver.scenario_011_orders_current
SHALLOW CLONE dbx_lab_dev.silver.orders_current;

-- 5. Vérifier la baseline de la cible isolée.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders
FROM dbx_lab_dev.silver.scenario_011_orders_current;

-- 6. Après le premier MERGE Python : attendu 50005 lignes.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders
FROM dbx_lab_dev.silver.scenario_011_orders_current;

-- 7. Vérifier les trois INSERT.
SELECT
    order_id,
    customer_id,
    order_date,
    status
FROM dbx_lab_dev.silver.scenario_011_orders_current
WHERE order_id IN (960001, 960002, 960003)
ORDER BY order_id;

-- 8. Vérifier l'absence de doublons sur les trois clés après replay.
SELECT
    order_id,
    COUNT(*) AS cnt
FROM dbx_lab_dev.silver.scenario_011_orders_current
WHERE order_id IN (960001, 960002, 960003)
GROUP BY order_id
ORDER BY order_id;

-- 9. Contrôle global d'idempotence.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows
FROM dbx_lab_dev.silver.scenario_011_orders_current;

-- 10. Contrôle PASS / FAIL du scénario.
SELECT
    CASE
        WHEN COUNT(*) = 50005
         AND COUNT(DISTINCT order_id) = 50005
        THEN 'PASS'
        ELSE 'FAIL'
    END AS scenario_011_result
FROM dbx_lab_dev.silver.scenario_011_orders_current;
