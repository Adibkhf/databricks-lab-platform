-- system.lakeflow.job_run_timeline peut contenir plusieurs lignes pour un même run.
-- Pour les jobs multi-tasks, les colonnes run_duration_seconds / queue_duration_seconds
-- de cette table valent 0. On calcule donc nous-mêmes la durée réelle du run.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.job_runs AS

WITH latest_jobs AS (

    -- Récupère la configuration actuelle de chaque Job.
    SELECT
        workspace_id,
        job_id,
        name AS job_name,
        creator_user_name,
        run_as_user_name,
        trigger_type AS configured_trigger_type,
        paused

    FROM system.lakeflow.jobs

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, job_id
        ORDER BY change_time DESC
    ) = 1
),

runs AS (

    SELECT
        workspace_id,
        job_id,
        run_id,

        MIN(period_start_time) AS run_start_time,
        MAX(period_end_time) AS run_end_time,

        MAX(trigger_type) AS trigger_type,
        MAX(run_type) AS run_type,
        MAX(run_name) AS run_name,

        MAX(result_state) AS result_state,
        MAX(termination_code) AS termination_code,
        MAX(termination_type) AS termination_type,

        -- Durée réelle du run calculée à partir des périodes de timeline.
        CAST(
            SUM(period_end_time - period_start_time) AS LONG
        ) AS run_duration_seconds,

        -- Ces métriques peuvent valoir 0 pour les jobs multi-tasks.
        MAX(setup_duration_seconds) AS setup_duration_seconds,
        MAX(queue_duration_seconds) AS queue_duration_seconds,
        MAX(execution_duration_seconds) AS execution_duration_seconds,
        MAX(cleanup_duration_seconds) AS cleanup_duration_seconds,

        GREATEST(
            COUNT_IF(result_state IS NOT NULL) - 1,
            0
        ) AS repair_count

    FROM system.lakeflow.job_run_timeline

    GROUP BY
        workspace_id,
        job_id,
        run_id
)

SELECT
    runs.workspace_id,
    runs.job_id,
    jobs.job_name,

    runs.run_id,
    DATE(runs.run_start_time) AS run_date,

    runs.run_start_time,
    runs.run_end_time,

    runs.trigger_type,
    runs.run_type,
    runs.run_name,

    runs.result_state,
    runs.termination_code,
    runs.termination_type,

    runs.setup_duration_seconds,
    runs.queue_duration_seconds,
    runs.execution_duration_seconds,
    runs.cleanup_duration_seconds,

    runs.run_duration_seconds,

    runs.repair_count,

    jobs.creator_user_name,
    jobs.run_as_user_name,
    jobs.paused

FROM runs

LEFT JOIN latest_jobs AS jobs
    ON runs.workspace_id = jobs.workspace_id
    AND runs.job_id = jobs.job_id;