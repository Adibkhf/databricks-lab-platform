%sql
-- system.billing.usage contient la consommation facturable.
-- usage_metadata.job_id et job_run_id permettent d'attribuer le coût
-- à un Job et à une exécution précise lorsqu'ils sont renseignés.

-- system.lakeflow.jobs contient la définition et l'historique des Jobs.
-- Cette table est SCD2 : un changement de configuration crée une nouvelle ligne.
-- On récupère ici la version la plus récente uniquement.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.finops_cost_by_job AS

WITH latest_jobs AS (

    SELECT
        workspace_id,
        job_id,
        name AS job_name,
        creator_user_name,
        run_as_user_name,
        trigger_type,
        paused

    FROM (

        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id
                ORDER BY change_time DESC
            ) AS rn

        FROM system.lakeflow.jobs

    )

    -- Une seule ligne représente l'état actuel de chaque Job.
    WHERE rn = 1
      AND delete_time IS NULL
),

priced_job_usage AS (

    SELECT
        usage.usage_date,
        usage.workspace_id,

        usage.usage_metadata.job_id AS job_id,
        usage.usage_metadata.job_run_id AS job_run_id,

        usage.identity_metadata.run_as AS billed_run_as,

        usage.sku_name,
        usage.usage_unit,
        usage.usage_quantity,

        -- Convertit les unités consommées en coût catalogue USD.
        usage.usage_quantity
            * prices.pricing.effective_list.default AS list_cost_usd

    FROM system.billing.usage AS usage

    INNER JOIN system.billing.list_prices AS prices
        ON usage.sku_name = prices.sku_name
        AND usage.cloud = prices.cloud
        AND usage.usage_unit = prices.usage_unit

        -- Sélectionne le tarif qui était réellement actif
        -- au moment où cette consommation a eu lieu.
        AND usage.usage_end_time >= prices.price_start_time
        AND (
            prices.price_end_time IS NULL
            OR usage.usage_end_time < prices.price_end_time
        )

    WHERE usage.billing_origin_product = 'JOBS'

      -- Sans job_id, on ne peut pas attribuer précisément
      -- cette consommation à un Job individuel.
      AND usage.usage_metadata.job_id IS NOT NULL
)

SELECT
    usage.usage_date,
    usage.workspace_id,

    usage.job_id,
    jobs.job_name,

    jobs.creator_user_name,
    jobs.run_as_user_name,
    jobs.trigger_type,
    jobs.paused,

    usage.sku_name,
    usage.usage_unit,

    -- Nombre de runs distincts ayant généré une consommation ce jour-là.
    COUNT(DISTINCT usage.job_run_id) AS run_count,

    ROUND(
        SUM(usage.usage_quantity),
        3
    ) AS total_usage_quantity,

    ROUND(
        SUM(usage.list_cost_usd),
        2
    ) AS list_cost_usd

FROM priced_job_usage AS usage

LEFT JOIN latest_jobs AS jobs
    ON usage.workspace_id = jobs.workspace_id
    AND usage.job_id = jobs.job_id

GROUP BY
    usage.usage_date,
    usage.workspace_id,
    usage.job_id,
    jobs.job_name,
    jobs.creator_user_name,
    jobs.run_as_user_name,
    jobs.trigger_type,
    jobs.paused,
    usage.sku_name,
    usage.usage_unit;