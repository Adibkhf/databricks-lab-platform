# ============================================================
# Scenario 025 - Unbounded State Store
# ============================================================

import time


# ============================================================
# CONFIGURATION
# ============================================================

# Checkpoint stocké dans un Volume Unity Catalog.
# Ne pas utiliser /tmp ou dbfs:/tmp sur ce workspace.
CHECKPOINT_PATH = "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/scenario_025_unbounded_state_v2"

QUERY_NAME = "scenario_025_unbounded_state"


# ============================================================
# SOURCE STREAMING
# ============================================================

# Génère 500 événements par seconde.
# `value` augmente continuellement : chaque événement produit
# donc un nouveau customer_id.
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


# ============================================================
# AGRÉGATION STATEFUL SANS WATERMARK
# ============================================================

# Aucun watermark / aucune borne temporelle.
# Chaque customer_id rencontré reste dans le State Store.
aggregated = (
    events
    .groupBy("customer_id")
    .count()
)


# ============================================================
# STREAMING QUERY
# ============================================================

query = (
    aggregated.writeStream
    .format("memory")
    .queryName(QUERY_NAME)
    .outputMode("update")

    # Checkpoint explicite obligatoire sur ce workspace.
    .option("checkpointLocation", CHECKPOINT_PATH)

    # Un micro-batch environ toutes les 5 secondes.
    .trigger(processingTime="5 seconds")
    .start()
)


# ============================================================
# OBSERVATION DU STATE STORE
# ============================================================

try:

    # Observer plusieurs micro-batches.
    for _ in range(8):

        time.sleep(5)

        progress = query.lastProgress

        if not progress:
            print("Waiting for first micro-batch...")
            continue

        state_operators = progress.get("stateOperators", [])

        if not state_operators:
            print(
                f"BATCH={progress.get('batchId')} "
                "- no state metrics available yet"
            )
            continue

        state = state_operators[0]

        print("========================================")
        print(f"BATCH={progress.get('batchId')}")
        print(
            f"INPUT_ROWS="
            f"{progress.get('numInputRows', 0)}"
        )
        print(
            f"STATE_ROWS="
            f"{state.get('numRowsTotal', 0)}"
        )
        print(
            f"STATE_UPDATED_ROWS="
            f"{state.get('numRowsUpdated', 0)}"
        )
        print(
            f"STATE_MEMORY_BYTES="
            f"{state.get('memoryUsedBytes', 0)}"
        )
        print(
            f"BATCH_DURATION_MS="
            f"{progress.get('durationMs', {}).get('triggerExecution', 0)}"
        )
        print("========================================")


finally:

    # Toujours arrêter proprement le stream.
    if query.isActive:
        query.stop()

    print("Scenario 025 stream stopped.")