-- FinOps — Coût Databricks quotidien par workspace

CREATE SCHEMA IF NOT EXISTS dbx_lab_dev.monitoring;

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.finops_cost_daily AS

SELECT
    usage.usage_date,
    usage.workspace_id,
    ROUND(SUM(usage.usage_quantity), 3) AS total_dbu,
    ROUND(
        SUM(
            usage.usage_quantity
            * prices.pricing.effective_list.default
        ),
        2
    ) AS list_cost_usd
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

GROUP BY
    usage.usage_date,
    usage.workspace_id;

-- FinOps — Vérification de l'évolution quotidienne des coûts

SELECT *
FROM dbx_lab_dev.monitoring.finops_cost_daily
ORDER BY usage_date DESC;