# ============================================================
# Scenario 028 - Slow Micro-Batch
# ============================================================

import time

from pyspark.sql import functions as F
from pyspark.sql.types import LongType


# ============================================================
# CONFIGURATION
# ============================================================

spark.conf.set("spark.sql.shuffle.partitions", "8")

CHECKPOINT_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/"
    "scenario_028_slow_microbatch_v1"
)

QUERY_NAME = "scenario_028_slow_microbatch"

TRIGGER_SECONDS = 5


# ============================================================
# TRANSFORMATION VOLONTAIREMENT LENTE
# ============================================================

# Anti-pattern volontaire :
# appel Python ligne par ligne + délai artificiel.
@F.udf(returnType=LongType())
def slow_transform(value):
    time.sleep(0.01)
    return value * 2


# ============================================================
# SOURCE STREAMING
# ============================================================

events = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 500)
    .load()
    .select(
        F.col("timestamp").alias("event_time"),
        F.col("value").alias("transaction_id")
    )
)


# ============================================================
# BRONZE -> SILVER SIMULE
# ============================================================

silver = (
    events
    .withColumn(
        "processed_value",
        slow_transform(F.col("transaction_id"))
    )
)


# ============================================================
# STREAM
# ============================================================

query = (
    silver.writeStream
    .format("memory")
    .queryName(QUERY_NAME)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .start()
)

print("")
print("STREAM STARTED")
print("Trigger = 5 seconds")
print("Source = 500 rows/second")
print("")


# ============================================================
# MONITORING
# ============================================================

last_batch_id = None
completed_batches = 0

MAX_BATCHES = 6
MAX_OBSERVATION_SECONDS = 240

start_time = time.time()


try:

    while completed_batches < MAX_BATCHES:

        time.sleep(2)

        if time.time() - start_time > MAX_OBSERVATION_SECONDS:
            print("Observation timeout reached.")
            break

        progress = query.lastProgress

        if not progress:
            continue

        batch_id = progress.get("batchId")

        if batch_id == last_batch_id:
            continue

        last_batch_id = batch_id
        completed_batches += 1

        input_rows = progress.get("numInputRows", 0)

        input_rate = progress.get(
            "inputRowsPerSecond",
            0
        )

        processing_rate = progress.get(
            "processedRowsPerSecond",
            0
        )

        batch_duration_ms = (
            progress
            .get("durationMs", {})
            .get("triggerExecution", 0)
        )

        batch_duration_seconds = (
            batch_duration_ms / 1000
        )

        print("========================================")
        print(f"BATCH={batch_id}")

        print(
            f"INPUT_ROWS="
            f"{input_rows}"
        )

        print(
            f"INPUT_RATE_ROWS_SEC="
            f"{input_rate:.2f}"
        )

        print(
            f"PROCESSING_RATE_ROWS_SEC="
            f"{processing_rate:.2f}"
        )

        print(
            f"BATCH_DURATION_SEC="
            f"{batch_duration_seconds:.2f}"
        )

        print(
            f"TRIGGER_SEC="
            f"{TRIGGER_SECONDS}"
        )

        if batch_duration_seconds > TRIGGER_SECONDS:
            print("STATUS=FALLING_BEHIND")
        else:
            print("STATUS=KEEPING_UP")

        print("========================================")


finally:

    if query.isActive:
        query.stop()

    print("")
    print("Scenario 028 slow stream stopped.")