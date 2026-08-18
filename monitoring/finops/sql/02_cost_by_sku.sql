-- FinOps — Analyse quotidienne des coûts par SKU Databricks
-- system.billing.usage contient chaque unité de consommation facturable Databricks :
-- workspace, SKU, produit, quantité consommée, unité et période d'utilisation.

-- system.billing.list_prices contient l'historique des prix catalogue de chaque SKU.
-- La jointure temporelle permet d'utiliser le prix réellement applicable au moment de la consommation.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.finops_cost_by_sku AS

SELECT
    usage.usage_date,                                                   -- Jour de consommation pour analyser l'évolution dans le temps.
    usage.workspace_id,                                                -- Workspace responsable de la consommation.
    usage.billing_origin_product,                                      -- Produit ayant généré l'usage : JOBS, ALL_PURPOSE, etc.
    usage.sku_name,                                                     -- SKU facturé par Databricks.
    usage.usage_unit,                                                   -- Unité facturée : généralement DBU, mais d'autres unités existent.
    
    ROUND(
        SUM(usage.usage_quantity),
        3
    ) AS total_usage_quantity,                                         -- Quantité totale consommée pour ce SKU.

    ROUND(
        SUM(
            usage.usage_quantity
            * prices.pricing.effective_list.default
        ),
        2
    ) AS list_cost_usd                                                 -- Coût catalogue estimé au prix applicable à cette période.

FROM system.billing.usage AS usage

INNER JOIN system.billing.list_prices AS prices
    ON usage.sku_name = prices.sku_name                                -- Même SKU.
    AND usage.cloud = prices.cloud                                     -- Même cloud : ici GCP.
    AND usage.usage_unit = prices.usage_unit                            -- Même unité de facturation.
    AND usage.usage_end_time >= prices.price_start_time                 -- Le prix était déjà actif pendant cette consommation.
    AND (
        prices.price_end_time IS NULL
        OR usage.usage_end_time < prices.price_end_time                 -- Et n'avait pas encore expiré.
    )

GROUP BY
    usage.usage_date,
    usage.workspace_id,
    usage.billing_origin_product,
    usage.sku_name,
    usage.usage_unit;