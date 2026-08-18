-- system.billing.usage contient la consommation facturable.
-- usage_metadata.cluster_id permet d'identifier le Classic Compute responsable.
-- system.compute.clusters est historisée : un cluster peut changer de taille, owner,
-- runtime ou autoscaling au cours de sa vie. On doit donc retrouver la configuration
-- qui était active au moment exact de la consommation.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.finops_cost_by_compute AS

WITH billing_with_price AS (

    SELECT
        usage.record_id,
        usage.workspace_id,
        usage.usage_date,
        usage.usage_start_time,
        usage.usage_end_time,

        usage.usage_metadata.cluster_id AS cluster_id,
        usage.usage_metadata.node_type AS billed_node_type,

        usage.sku_name,
        usage.usage_unit,
        usage.usage_quantity,

        usage.usage_quantity
            * prices.pricing.effective_list.default AS list_cost_usd

    FROM system.billing.usage AS usage

    INNER JOIN system.billing.list_prices AS prices
        ON usage.sku_name = prices.sku_name
        AND usage.cloud = prices.cloud
        AND usage.usage_unit = prices.usage_unit
        AND usage.usage_end_time >= prices.price_start_time
        AND (
            prices.price_end_time IS NULL
            OR usage.usage_end_time < prices.price_end_time
        )

    -- Cette vue concerne uniquement les Classic Compute ayant un cluster_id.
    -- Les workloads Serverless seront analysés séparément via job_id,
    -- warehouse_id, pipeline_id, etc.
    WHERE usage.usage_metadata.cluster_id IS NOT NULL
),

billing_with_cluster_config AS (

    SELECT
        b.*,

        c.cluster_name,
        c.owned_by,
        c.cluster_source,
        c.driver_node_type,
        c.worker_node_type,
        c.worker_count,
        c.min_autoscale_workers,
        c.max_autoscale_workers,
        c.dbr_version,
        c.data_security_mode,
        c.policy_id,
        c.change_time,

        -- Un cluster peut avoir plusieurs versions de configuration.
        -- On classe celles qui existaient déjà au moment de la consommation,
        -- de la plus récente à la plus ancienne.
        ROW_NUMBER() OVER (
            PARTITION BY b.record_id
            ORDER BY c.change_time DESC
        ) AS config_rank

    FROM billing_with_price AS b

    LEFT JOIN system.compute.clusters AS c
        ON b.workspace_id = c.workspace_id
        AND b.cluster_id = c.cluster_id
        AND c.change_time <= b.usage_start_time
)

SELECT
    usage_date,
    workspace_id,
    cluster_id,
    cluster_name,
    owned_by,
    cluster_source,

    driver_node_type,
    worker_node_type,

    worker_count,
    min_autoscale_workers,
    max_autoscale_workers,

    dbr_version,
    data_security_mode,
    policy_id,

    sku_name,
    billed_node_type,
    usage_unit,

    ROUND(SUM(usage_quantity), 3) AS total_usage_quantity,
    ROUND(SUM(list_cost_usd), 2) AS list_cost_usd

FROM billing_with_cluster_config

-- On garde uniquement la configuration du cluster active
-- au moment où le record de billing a été généré.
WHERE config_rank = 1

GROUP BY
    usage_date,
    workspace_id,
    cluster_id,
    cluster_name,
    owned_by,
    cluster_source,
    driver_node_type,
    worker_node_type,
    worker_count,
    min_autoscale_workers,
    max_autoscale_workers,
    dbr_version,
    data_security_mode,
    policy_id,
    sku_name,
    billed_node_type,
    usage_unit;