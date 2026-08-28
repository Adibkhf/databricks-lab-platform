-- ============================================================
-- Scenario 028 - Functional checks
-- ============================================================
-- Memory sink: execute in the SAME SparkSession before query.stop().

-- 1. Confirm output exists.
SELECT COUNT(*) AS output_rows
FROM scenario_028_fast_microbatch;

-- 2. Confirm optimized logic is identical to the expected business rule.
SELECT COUNT(*) AS invalid_rows
FROM scenario_028_fast_microbatch
WHERE processed_value <> transaction_id * 2;

-- 3. Final validation.
SELECT
    CASE
        WHEN COUNT(*) > 0
         AND SUM(
             CASE WHEN processed_value <> transaction_id * 2 THEN 1 ELSE 0 END
         ) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS scenario_028_result
FROM scenario_028_fast_microbatch;

-- 4. Sample.
SELECT event_time, transaction_id, processed_value
FROM scenario_028_fast_microbatch
ORDER BY event_time DESC
LIMIT 20;
