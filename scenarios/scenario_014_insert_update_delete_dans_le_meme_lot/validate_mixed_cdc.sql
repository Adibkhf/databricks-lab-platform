-- Scenario 014 - Contrôles après exécution de apply_mixed_cdc.py.

-- Bronze conserve les sept événements : la déduplication ne modifie pas la source.
SELECT * FROM dbx_lab_dev.bronze.scenario_014_transactions_cdc
ORDER BY transaction_id, sequence_num;

-- Quatre clés dans l'état Silver, dont 990002 supprimée logiquement en v2.
SELECT transaction_id, payment_id, amount, sequence_num, _is_deleted, _applied_at
FROM dbx_lab_dev.silver.scenario_014_transactions_state
ORDER BY transaction_id;

-- Trois transactions actives : 990001 (120), 990003 (30), 990004 (40).
SELECT * FROM dbx_lab_dev.silver.scenario_014_transactions_current
ORDER BY transaction_id;

-- Trois lignes et trois clés distinctes attendues dans la vue active.
SELECT COUNT(*) AS active_rows, COUNT(DISTINCT transaction_id) AS distinct_transactions
FROM dbx_lab_dev.silver.scenario_014_transactions_current;

-- Gold : trois transactions, montant total 190.00.
SELECT * FROM dbx_lab_dev.gold.scenario_014_transactions_summary;

-- Premier MERGE corrigé : 1 INSERT, 2 UPDATE, 0 DELETE physique.
-- Replay et ancien INSERT : aucun nouveau changement dans Delta.
DESCRIBE HISTORY dbx_lab_dev.silver.scenario_014_transactions_state;
