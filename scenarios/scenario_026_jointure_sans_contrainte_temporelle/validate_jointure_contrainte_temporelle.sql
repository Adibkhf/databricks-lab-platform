-- ============================================================
-- Scenario 026 - Checks
-- ============================================================
-- IMPORTANT:
-- scenario_026_bounded_join est un memory sink.
-- Ces requetes doivent etre executees dans la meme SparkSession
-- que le stream, avant query.stop().
-- Depuis le script Python, utiliser spark.sql(...) si necessaire.


-- 1. Nombre de lignes produites par la jointure
SELECT
    COUNT(*) AS matched_rows
FROM scenario_026_bounded_join;


-- 2. Vérifier la borne temporelle réellement observée
SELECT
    MIN(
        unix_timestamp(payment_time)
        - unix_timestamp(order_time)
    ) AS min_delay_seconds,
    MAX(
        unix_timestamp(payment_time)
        - unix_timestamp(order_time)
    ) AS max_delay_seconds
FROM scenario_026_bounded_join;


-- 3. Chercher d'éventuels matches hors de la plage autorisée
SELECT
    COUNT(*) AS invalid_matches
FROM scenario_026_bounded_join
WHERE payment_time < order_time
   OR payment_time > order_time + INTERVAL 10 SECONDS;


-- 4. Validation finale du scénario
SELECT
    CASE
        WHEN COUNT(*) > 0
         AND MIN(
            unix_timestamp(payment_time)
            - unix_timestamp(order_time)
         ) >= 0
         AND MAX(
            unix_timestamp(payment_time)
            - unix_timestamp(order_time)
         ) <= 10
        THEN 'PASS'
        ELSE 'FAIL'
    END AS scenario_026_result
FROM scenario_026_bounded_join;


-- 5. Exemple de résultats pour inspection
SELECT
    order_id,
    order_time,
    payment_time,
    unix_timestamp(payment_time)
        - unix_timestamp(order_time) AS delay_seconds
FROM scenario_026_bounded_join
ORDER BY order_time DESC
LIMIT 20;
