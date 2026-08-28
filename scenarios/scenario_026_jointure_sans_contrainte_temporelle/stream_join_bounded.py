# ============================================================
# Scenario 026 - Bounded Stream-Stream Join + Validation
# ============================================================

import time

from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

spark.conf.set("spark.sql.shuffle.partitions", "8")

CHECKPOINT_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/"
    "scenario_026_stream_join_bounded_v2"
)

QUERY_NAME = "scenario_026_bounded_join"


# ============================================================
# ORDERS STREAM
# ============================================================

orders = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
    .select(
        F.col("timestamp").alias("order_time"),
        F.col("value").alias("order_id")
    )
    .withWatermark("order_time", "10 seconds")
)


# ============================================================
# PAYMENTS STREAM
# ============================================================

payments = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 70)
    .load()
    .select(
        F.col("timestamp").alias("payment_time"),
        F.col("value").alias("order_id")
    )
    .withWatermark("payment_time", "10 seconds")
)


# ============================================================
# STREAM-STREAM JOIN BORNEE
# ============================================================

join_condition = (
    (F.col("o.order_id") == F.col("p.order_id"))
    &
    (F.col("p.payment_time") >= F.col("o.order_time"))
    &
    (
        F.col("p.payment_time")
        <= F.col("o.order_time") + F.expr("INTERVAL 10 SECONDS")
    )
)

joined = (
    orders.alias("o")
    .join(
        payments.alias("p"),
        join_condition,
        "inner"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.order_time"),
        F.col("p.payment_time")
    )
)


# ============================================================
# STREAMING QUERY
# ============================================================

query = (
    joined.writeStream
    .format("memory")
    .queryName(QUERY_NAME)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

print("")
print("STREAM STARTED")
print("")


# ============================================================
# MONITORING
# ============================================================

last_batch_id = None
completed_batches = 0

MAX_OBSERVATION_SECONDS = 240
start_time = time.time()


try:

    # Observer 8 vrais micro-batches.
    while completed_batches < 8:

        time.sleep(2)

        if time.time() - start_time > MAX_OBSERVATION_SECONDS:
            print("Observation timeout reached.")
            break

        progress = query.lastProgress

        if not progress:
            continue

        batch_id = progress.get("batchId")

        # Evite d'afficher plusieurs fois le même batch.
        if batch_id == last_batch_id:
            continue

        last_batch_id = batch_id

        state_operators = progress.get("stateOperators", [])

        if not state_operators:
            continue

        completed_batches += 1

        state = state_operators[0]

        print("========================================")
        print(f"BATCH={batch_id}")
        print(f"INPUT_ROWS={progress.get('numInputRows', 0)}")
        print(f"STATE_ROWS={state.get('numRowsTotal', 0)}")
        print(f"STATE_UPDATED_ROWS={state.get('numRowsUpdated', 0)}")
        print(f"STATE_REMOVED_ROWS={state.get('numRowsRemoved', 0)}")
        print(f"STATE_MEMORY_BYTES={state.get('memoryUsedBytes', 0)}")
        print(
            f"ROWS_DROPPED_BY_WATERMARK="
            f"{state.get('numRowsDroppedByWatermark', 0)}"
        )
        print(
            f"BATCH_DURATION_MS="
            f"{progress.get('durationMs', {}).get('triggerExecution', 0)}"
        )
        print("========================================")


    # ========================================================
    # VALIDATION DE LA JOINTURE
    # ========================================================

    print("")
    print("========================================")
    print("JOIN VALIDATION")
    print("========================================")

    validation = spark.sql(f"""
        SELECT
            COUNT(*) AS matched_rows,
            MIN(
                unix_timestamp(payment_time)
                - unix_timestamp(order_time)
            ) AS min_delay_seconds,
            MAX(
                unix_timestamp(payment_time)
                - unix_timestamp(order_time)
            ) AS max_delay_seconds
        FROM {QUERY_NAME}
    """)

    validation.show(truncate=False)


    # ========================================================
    # CHECK FINAL
    # ========================================================

    check = spark.sql(f"""
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
        FROM {QUERY_NAME}
    """)

    check.show(truncate=False)


finally:

    if query.isActive:
        query.stop()

    print("")
    print("Scenario 026 bounded stream stopped.")