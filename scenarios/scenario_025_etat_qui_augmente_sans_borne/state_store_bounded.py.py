# ============================================================
# Scenario 025 - Bounded State Store
# ============================================================

import time
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

# Petit nombre de partitions adapté au cluster de test.
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Nouveau checkpoint pour repartir avec un état propre.
CHECKPOINT_PATH = (
    "/Volumes/dbx_lab_dev/landing/raw/_checkpoints/"
    "scenario_025_bounded_state_v3"
)

QUERY_NAME = "scenario_025_bounded_state"


# ============================================================
# SOURCE STREAMING
# ============================================================

# 200 événements générés par seconde.
events = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 200)
    .load()
    .select(
        F.col("timestamp").alias("event_time"),

        # Seulement 1000 customer_id possibles.
        # Les mêmes clés reviennent donc régulièrement.
        (F.col("value") % 1000)
        .cast("string")
        .alias("customer_id")
    )
)


# ============================================================
# STATE STORE BORNE
# ============================================================

aggregated = (
    events

    # Spark considère qu'une donnée située plus de 10 secondes
    # derrière le watermark devient trop ancienne.
    .withWatermark("event_time", "10 seconds")

    # Chaque état appartient à une fenêtre de 10 secondes.
    # Les anciennes fenêtres pourront donc être supprimées.
    .groupBy(
        F.window("event_time", "10 seconds"),
        F.col("customer_id")
    )
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
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

print("")
print("STREAM STARTED")
print("")
print("Waiting for completed micro-batches...")


# ============================================================
# MONITORING
# ============================================================

last_batch_id = None
completed_batches = 0

# Sécurité : arrêt automatique après 4 minutes maximum.
MAX_OBSERVATION_SECONDS = 240
start_time = time.time()


try:

    # On attend 8 VRAIS micro-batches différents.
    while completed_batches < 8:

        time.sleep(2)

        # Evite de laisser tourner le test indéfiniment.
        if time.time() - start_time > MAX_OBSERVATION_SECONDS:
            print("")
            print("Observation timeout reached.")
            break

        progress = query.lastProgress

        # Aucun batch terminé pour l'instant.
        if not progress:
            continue

        batch_id = progress.get("batchId")

        # Evite d'afficher plusieurs fois le même lastProgress.
        if batch_id == last_batch_id:
            continue

        last_batch_id = batch_id

        state_operators = progress.get("stateOperators", [])

        if not state_operators:
            continue

        completed_batches += 1

        state = state_operators[0]

        print("")
        print("========================================")
        print(f"BATCH={batch_id}")

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
            f"STATE_REMOVED_ROWS="
            f"{state.get('numRowsRemoved', 0)}"
        )

        print(
            f"STATE_MEMORY_BYTES="
            f"{state.get('memoryUsedBytes', 0)}"
        )

        print(
            f"ROWS_DROPPED_BY_WATERMARK="
            f"{state.get('numRowsDroppedByWatermark', 0)}"
        )

        print(
            f"BATCH_DURATION_MS="
            f"{progress.get('durationMs', {}).get('triggerExecution', 0)}"
        )

        print("========================================")


finally:

    # Arrêt propre du stream.
    if query.isActive:
        query.stop()

    print("")
    print("Scenario 025 bounded stream stopped.")