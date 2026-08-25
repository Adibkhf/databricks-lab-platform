-- Cette vue produit les KPI principaux qui seront affichés
-- en haut de la page FinOps du futur Architect Control Center.
--
-- system.billing.usage = consommation réellement enregistrée par Databricks.
-- system.billing.list_prices = prix catalogue historique applicable à chaque SKU.
--
-- Important : les System Tables ne sont pas temps réel.
-- cost_today peut donc être partiel ; latest_usage_date permet de voir
-- jusqu'à quelle date les données de billing sont effectivement disponibles.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.finops_cost_kpis AS

WITH priced_usage AS (

    SELECT
        usage.workspace_id,
        usage.usage_date,
        usage.usage_unit,
        usage.usage_quantity,

        -- Conversion de la consommation en coût catalogue
        -- avec le tarif applicable au moment où elle a eu lieu.
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

aggregated AS (

    SELECT
        workspace_id,

        -- Permet de détecter un éventuel retard des données de billing.
        MAX(usage_date) AS latest_usage_date,

        -- Coût enregistré aujourd'hui.
        SUM(
            CASE
                WHEN usage_date = CURRENT_DATE()
                THEN list_cost_usd
                ELSE 0
            END
        ) AS cost_today,

        -- DBU uniquement : les autres unités de billing sont volontairement exclues.
        SUM(
            CASE
                WHEN usage_date = CURRENT_DATE()
                     AND usage_unit = 'DBU'
                THEN usage_quantity
                ELSE 0
            END
        ) AS dbu_today,

        -- Month-To-Date : coût depuis le premier jour du mois courant.
        SUM(
            CASE
                WHEN usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
                     AND usage_date <= CURRENT_DATE()
                THEN list_cost_usd
                ELSE 0
            END
        ) AS cost_mtd,

        -- DBU consommées depuis le début du mois.
        SUM(
            CASE
                WHEN usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
                     AND usage_date <= CURRENT_DATE()
                     AND usage_unit = 'DBU'
                THEN usage_quantity
                ELSE 0
            END
        ) AS dbu_mtd,

        -- Les 7 derniers jours permettent de détecter rapidement une dérive récente.
        SUM(
            CASE
                WHEN usage_date BETWEEN DATE_SUB(CURRENT_DATE(), 6) AND CURRENT_DATE()
                THEN list_cost_usd
                ELSE 0
            END
        ) AS cost_last_7_days,

        -- Fenêtre précédente de 7 jours pour comparer deux périodes équivalentes.
        SUM(
            CASE
                WHEN usage_date BETWEEN DATE_SUB(CURRENT_DATE(), 13)
                                    AND DATE_SUB(CURRENT_DATE(), 7)
                THEN list_cost_usd
                ELSE 0
            END
        ) AS cost_previous_7_days,

        -- Mois précédent complet : utile comme baseline mensuelle simple.
        SUM(
            CASE
                WHEN usage_date >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE()), -1)
                     AND usage_date < DATE_TRUNC('MONTH', CURRENT_DATE())
                THEN list_cost_usd
                ELSE 0
            END
        ) AS previous_month_cost

    FROM priced_usage

    GROUP BY workspace_id
),

metrics AS (

    SELECT
        *,

        -- Nombre de jours du mois pour lesquels nous avons potentiellement
        -- accumulé de la consommation.
        CASE
            WHEN latest_usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
            THEN DATEDIFF(
                LEAST(latest_usage_date, CURRENT_DATE()),
                DATE_TRUNC('MONTH', CURRENT_DATE())
            ) + 1
            ELSE 0
        END AS observed_days_mtd

    FROM aggregated
)

SELECT
    workspace_id,

    CURRENT_DATE() AS dashboard_date,
    latest_usage_date,

    ROUND(cost_today, 2) AS cost_today_usd,
    ROUND(dbu_today, 3) AS dbu_today,

    ROUND(cost_mtd, 2) AS cost_mtd_usd,
    ROUND(dbu_mtd, 3) AS dbu_mtd,

    ROUND(cost_last_7_days, 2) AS cost_last_7_days_usd,
    ROUND(cost_previous_7_days, 2) AS cost_previous_7_days_usd,

    -- Variation récente : permet de détecter rapidement une accélération du coût.
    ROUND(
        100 * (cost_last_7_days - cost_previous_7_days)
        / NULLIF(cost_previous_7_days, 0),
        2
    ) AS cost_7d_change_pct,

    -- Coût quotidien moyen observé depuis le début du mois.
    ROUND(
        cost_mtd / NULLIF(observed_days_mtd, 0),
        2
    ) AS avg_daily_cost_mtd_usd,

    -- Projection simple :
    -- coût moyen observé × nombre total de jours du mois.
    -- Ce n'est pas une prévision financière avancée, mais un run-rate.
    ROUND(
        (
            cost_mtd
            / NULLIF(observed_days_mtd, 0)
        )
        * DAY(LAST_DAY(CURRENT_DATE())),
        2
    ) AS forecast_month_end_usd,

    ROUND(previous_month_cost, 2) AS previous_month_cost_usd

FROM metrics;