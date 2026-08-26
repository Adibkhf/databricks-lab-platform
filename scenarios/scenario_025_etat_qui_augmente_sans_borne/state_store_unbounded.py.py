# ============================================================
# Scenario 025 - Unbounded State Store
# ============================================================

import time

# Source streaming synthétique.
# Chaque ligne aura une nouvelle clé -> le state ne peut que grossir.
events = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 500)
    .load()
    .selectExpr(
        "timestamp AS event_time",
        "CAST(value AS STRING) AS customer_id"
    )
)

# Agrégation stateful SANS watermark.
# Chaque customer_id unique reste conservé dans le State Store.
aggregated = (
    events
    .groupBy("customer_id")
    .count()
)

query = (
    aggregated.writeStream
    .format("memory")
    .queryName("scenario_025_unbounded_state")
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)

# Observer environ 6 micro-batches.
for i in range(6):
    time.sleep(5)

    progress = query.lastProgress

    if progress and progress.get("stateOperators"):
        state = progress["stateOperators"][0]

        print("========================================")
        print(f"BATCH={progress['batchId']}")
        print(f"STATE_ROWS={state['numRowsTotal']}")
        print(f"STATE_MEMORY_BYTES={state['memoryUsedBytes']}")
        print(f"BATCH_DURATION_MS={progress['durationMs'].get('triggerExecution', 0)}")
        print("========================================")

query.stop()