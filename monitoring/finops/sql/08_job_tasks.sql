-- system.lakeflow.job_task_run_timeline descend au niveau Task.
-- Elle permet de trouver la task réellement responsable d'un Job lent ou échoué.
-- Comme pour les Jobs, une task longue peut être découpée en plusieurs périodes.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.job_tasks AS

WITH latest_jobs AS (

    SELECT
        workspace_id,
        job_id,
        name AS job_name

    FROM system.lakeflow.jobs

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, job_id
        ORDER BY change_time DESC
    ) = 1
),

tasks AS (

    SELECT
        workspace_id,
        job_id,

        job_run_id,
        run_id AS task_run_id,
        task_key,

        MIN(period_start_time) AS task_start_time,
        MAX(period_end_time) AS task_end_time,

        MAX(result_state) AS result_state,
        MAX(termination_code) AS termination_code,
        MAX(termination_type) AS termination_type,

        MAX(setup_duration_seconds) AS setup_duration_seconds,
        MAX(execution_duration_seconds) AS execution_duration_seconds,
        MAX(cleanup_duration_seconds) AS cleanup_duration_seconds,

        -- Un Job task peut utiliser cluster, interactive compute ou SQL Warehouse.
        FIRST(compute_ids, TRUE) AS compute_ids

    FROM system.lakeflow.job_task_run_timeline

    GROUP BY
        workspace_id,
        job_id,
        job_run_id,
        run_id,
        task_key
)

SELECT
    tasks.workspace_id,
    tasks.job_id,
    jobs.job_name,

    tasks.job_run_id,
    tasks.task_run_id,
    tasks.task_key,

    DATE(tasks.task_start_time) AS task_date,

    tasks.task_start_time,
    tasks.task_end_time,

    UNIX_TIMESTAMP(tasks.task_end_time)
        - UNIX_TIMESTAMP(tasks.task_start_time)
        AS task_duration_seconds,

    tasks.setup_duration_seconds,
    tasks.execution_duration_seconds,
    tasks.cleanup_duration_seconds,

    tasks.result_state,
    tasks.termination_code,
    tasks.termination_type,

    tasks.compute_ids

FROM tasks

LEFT JOIN latest_jobs AS jobs
    ON tasks.workspace_id = jobs.workspace_id
    AND tasks.job_id = jobs.job_id;