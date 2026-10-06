-- Scenario 013 - Contrôles après exécution de apply_delete_cdc.py.

-- Deux DELETE v2 : un récent pour 980001, un ancien pour 980002.
SELECT * FROM dbx_lab_dev.bronze.scenario_013_payments_cdc
ORDER BY payment_id;

-- Attendu : 980002 en version 3 et 980003 en version 1 ; 980001 absent.
SELECT payment_id, amount, sequence_num, _applied_at
FROM dbx_lab_dev.silver.scenario_013_payments_current
ORDER BY payment_id;

SELECT COUNT(*) AS total_rows, COUNT(DISTINCT payment_id) AS distinct_payments
FROM dbx_lab_dev.silver.scenario_013_payments_current;

-- Gold : deux paiements, montant total 80.00.
SELECT * FROM dbx_lab_dev.gold.scenario_013_payments_daily;

-- Une suppression puis un replay sans nouvelle suppression.
DESCRIBE HISTORY dbx_lab_dev.silver.scenario_013_payments_current;

-- Audit : DELETED / IGNORED_VERSION, puis ALREADY_ABSENT / IGNORED_VERSION.
SELECT event_id, payment_id, sequence_num, pass_name, outcome, _audited_at
FROM dbx_lab_dev.bronze.scenario_013_delete_audit
ORDER BY pass_name, event_id;
