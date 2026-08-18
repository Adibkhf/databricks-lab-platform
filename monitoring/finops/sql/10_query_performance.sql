-- system.query.history contient une ligne par statement SQL.
-- Elle permet de séparer le temps d'attente, compilation,
-- exécution réelle, scans, shuffle, spill et cache.
--
-- Attention : cette System Table couvre les SQL Warehouses
-- et le serverless compute pour notebooks/jobs.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.query_performance AS

SELECT
    workspace_id,
    statement_id,

    DATE(start_time) AS query_date,

    start_time,
    end_time,

    execution_status,
    statement_type,

    executed_by,
    executed_as,

    client_application,

    -- On conserve seulement un extrait afin de rendre le dashboard exploitable.
    LEFT(statement_text, 500) AS statement_preview,

    compute.type AS compute_type,
    compute.warehouse_id AS warehouse_id,
    compute.cluster_id AS cluster_id,

    total_duration_ms,
    waiting_for_compute_duration_ms,
    waiting_at_capacity_duration_ms,
    compilation_duration_ms,
    execution_duration_ms,
    total_task_duration_ms,

    -- Permet de distinguer une requête lente d'une requête
    -- qui a simplement attendu une capacité disponible.
    COALESCE(waiting_for_compute_duration_ms, 0)
        + COALESCE(waiting_at_capacity_duration_ms, 0)
        AS total_waiting_duration_ms,

    read_partitions,
    pruned_files,
    read_files,
    read_rows,
    read_bytes,

    produced_rows,

    read_io_cache_percent,
    from_result_cache,

    shuffle_read_bytes,
    spilled_local_bytes,

    written_bytes,
    written_rows,
    written_files,

    error_message,

    -- Attribution du SQL au workload Databricks qui l'a généré.
    query_source.job_info.job_id AS source_job_id,
    query_source.job_info.job_run_id AS source_job_run_id,
    query_source.job_info.job_task_run_id AS source_job_task_run_id,
    query_source.notebook_id AS source_notebook_id,
    query_source.dashboard_id AS source_dashboard_id

FROM system.query.history;