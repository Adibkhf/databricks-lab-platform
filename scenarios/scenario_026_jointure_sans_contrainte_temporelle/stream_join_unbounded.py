import time
from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", "8")

CHECKPOINT_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/"
    "scenario_026_stream_join_unbounded_v1"
)

# Flux orders : 100 événements/s.
orders = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
    .select(
        F.col("timestamp").alias("order_time"),
        F.col("value").alias("order_id")
    )
)

# Flux payments : 70 événements/s.
payments = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 70)
    .load()
    .select(
        F.col("timestamp").alias("payment_time"),
        F.col("value").alias("order_id")
    )
)

# Problème volontaire :
# jointure uniquement sur la clé métier, sans aucune borne temporelle.
joined = (
    orders.alias("o")
    .join(
        payments.alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "inner"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.order_time"),
        F.col("p.payment_time")
    )
)

query = (
    joined.writeStream
    .format("memory")
    .queryName("scenario_026_unbounded_join")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

print("STREAM STARTED")

last_batch_id = None
completed_batches = 0

try:
    while completed_batches < 6:
        time.sleep(2)

        progress = query.lastProgress

        if not progress:
            continue

        batch_id = progress.get("batchId")

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
        print(f"STATE_MEMORY_BYTES={state.get('memoryUsedBytes', 0)}")
        print(
            f"BATCH_DURATION_MS="
            f"{progress.get('durationMs', {}).get('triggerExecution', 0)}"
        )
        print("========================================")

finally:
    if query.isActive:
        query.stop()

    print("Scenario 026 stream stopped.")