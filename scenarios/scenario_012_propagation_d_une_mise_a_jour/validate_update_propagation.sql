-- Scenario 012 - Contrôles après exécution de apply_update_cdc.py.

-- Les quatre événements comprennent deux versions du client 970001.
SELECT *
FROM dbx_lab_dev.bronze.scenario_012_customers_cdc
ORDER BY customer_id, sequence_num;

-- Silver garde trois lignes et trois clés distinctes.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_rows
FROM dbx_lab_dev.silver.scenario_012_customers_current;

-- Attendu : 970001 BE v3, 970002 ES v2, 970003 FR v1.
SELECT customer_id, country, sequence_num, _applied_at
FROM dbx_lab_dev.silver.scenario_012_customers_current
ORDER BY customer_id;

-- Compteurs réels des deux MERGE UPDATE : 2 au premier passage, 0 au replay.
DESCRIBE HISTORY dbx_lab_dev.silver.scenario_012_customers_current;
