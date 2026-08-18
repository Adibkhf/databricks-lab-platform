-- system.billing.usage contient la consommation facturable Databricks.
-- custom_tags est une MAP<STRING, STRING> contenant les tags associés
-- à la ressource ayant généré la consommation.
--
-- Exemple :
-- {"environment":"dev", "team":"data", "project":"ecommerce"}
--
-- On transforme cette MAP en plusieurs lignes pour pouvoir analyser
-- indépendamment chaque couple tag_key / tag_value.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.finops_cost_by_tag AS

WITH priced_usage AS (

    SELECT
        usage.record_id,
        usage.usage_date,
        usage.workspace_id,
        usage.billing_origin_product,
        usage.sku_name,
        usage.usage_unit,
        usage.usage_quantity,
        usage.custom_tags,

        -- Prix catalogue correspondant exactement à la période de consommation.
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
),

exploded_tags AS (

    SELECT
        usage_date,
        workspace_id,
        billing_origin_product,
        sku_name,
        usage_unit,
        usage_quantity,
        list_cost_usd,

        -- map_entries transforme custom_tags en tableau de structures key/value.
        tag.key AS tag_key,
        tag.value AS tag_value

    FROM priced_usage

    -- EXPLODE produit une ligne pour chaque tag associé au record de billing.
    LATERAL VIEW EXPLODE(MAP_ENTRIES(custom_tags)) exploded AS tag
)

SELECT
    usage_date,
    workspace_id,

    tag_key,
    tag_value,

    billing_origin_product,
    sku_name,
    usage_unit,

    ROUND(SUM(usage_quantity), 3) AS total_usage_quantity,
    ROUND(SUM(list_cost_usd), 2) AS list_cost_usd

FROM exploded_tags

GROUP BY
    usage_date,
    workspace_id,
    tag_key,
    tag_value,
    billing_origin_product,
    sku_name,
    usage_unit;

  