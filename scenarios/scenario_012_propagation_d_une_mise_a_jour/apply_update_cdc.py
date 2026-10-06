import json
import time
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "dbx_lab_dev.bronze.scenario_012_customers_cdc"
TARGET_TABLE = "dbx_lab_dev.silver.scenario_012_customers_current"

# Le scénario utilise trois clients fictifs dans des tables dédiées.
spark.sql(f"""
    CREATE OR REPLACE TABLE {TARGET_TABLE} AS
    SELECT *, TIMESTAMP'2026-01-01 08:00:00' AS _applied_at
    FROM VALUES
        (970001L, 'FR', 1L),
        (970002L, 'IT', 1L),
        (970003L, 'FR', 1L)
    AS t(customer_id, country, sequence_num)
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SOURCE_TABLE} AS
    SELECT * FROM VALUES
        (970001L, 'DE', 2L, 'UPDATE'),
        (970001L, 'BE', 3L, 'UPDATE'),
        (970002L, 'ES', 2L, 'UPDATE'),
        (970003L, 'FR', 1L, 'UPDATE')
    AS t(customer_id, country, sequence_num, operation)
""")

source_df = spark.table(SOURCE_TABLE)

# Deux états différents pour la même clé et version rendent le lot ambigu.
invalid_rows = source_df.filter(
    "customer_id IS NULL OR country IS NULL OR sequence_num IS NULL "
    "OR sequence_num < 1 OR operation IS NULL OR operation <> 'UPDATE'"
).count()
assert invalid_rows == 0, "Le lot doit contenir des UPDATE complets et valides."

conflicts = (
    source_df.groupBy("customer_id", "sequence_num")
    .agg(F.countDistinct("country").alias("distinct_values"))
    .filter("distinct_values > 1")
    .count()
)
assert conflicts == 0, "Deux pays différents pour une même clé et version."

# Un seul événement par client doit parvenir au MERGE : sa dernière version.
latest_version = Window.partitionBy("customer_id").orderBy(F.desc("sequence_num"))
source_df = (
    source_df.dropDuplicates(["customer_id", "sequence_num", "country", "operation"])
    .withColumn("version_rank", F.row_number().over(latest_version))
    .filter("version_rank = 1")
    .drop("version_rank")
)

missing_customers = source_df.join(
    spark.table(TARGET_TABLE).select("customer_id"), "customer_id", "left_anti"
).count()
assert missing_customers == 0, "Ce cas UPDATE attend des clients déjà présents."


def snapshot():
    # collect est borné au petit jeu de trois clients du scénario.
    rows = spark.table(TARGET_TABLE).orderBy("customer_id").limit(4).collect()
    assert len(rows) == 3, "Silver doit conserver ses trois clients."
    assert len({row.customer_id for row in rows}) == 3, "Clé client dupliquée."
    return [row.asDict() for row in rows]


def merge_metrics(elapsed):
    # Les compteurs sont mesurés dans Delta, pas déduits du volume final.
    history = DeltaTable.forName(spark, TARGET_TABLE).history(1).first()
    assert history.operation == "MERGE", "La dernière écriture doit être le MERGE."
    metrics = history.operationMetrics
    return {
        "updated_rows": int(metrics.get("numTargetRowsUpdated", 0)),
        "inserted_rows": int(metrics.get("numTargetRowsInserted", 0)),
        "deleted_rows": int(metrics.get("numTargetRowsDeleted", 0)),
        "duration_seconds": round(elapsed, 3),
    }


baseline = snapshot()
target = DeltaTable.forName(spark, TARGET_TABLE)

# Reproduction : la logique INSERT du scénario 011 ignore les clés existantes.
start = time.perf_counter()
(
    target.alias("t")
    .merge(source_df.alias("s"), "t.customer_id = s.customer_id")
    .whenNotMatchedInsert(
        values={
            "customer_id": "s.customer_id",
            "country": "s.country",
            "sequence_num": "s.sequence_num",
            "_applied_at": "current_timestamp()",
        }
    )
    .execute()
)
insert_only_metrics = merge_metrics(time.perf_counter() - start)
assert snapshot() == baseline, "Le MERGE INSERT seul ne doit changer aucune clé existante."


def apply_updates():
    start = time.perf_counter()
    (
        target.alias("t")
        .merge(source_df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdate(
            condition="s.sequence_num > t.sequence_num",
            set={
                "country": "s.country",
                "sequence_num": "s.sequence_num",
                "_applied_at": "current_timestamp()",
            }
        )
        .execute()
    )
    return merge_metrics(time.perf_counter() - start)


# Premier passage : BE version 3, ES version 2, témoin FR version 1.
first_run = apply_updates()
after_update = snapshot()
expected = [(970001, "BE", 3), (970002, "ES", 2), (970003, "FR", 1)]
assert [(row["customer_id"], row["country"], row["sequence_num"]) for row in after_update] == expected
assert first_run["updated_rows"] == 2
assert first_run["inserted_rows"] == 0 and first_run["deleted_rows"] == 0
assert after_update[2] == baseline[2], "Le client témoin ne doit pas changer."
assert all(after_update[i]["_applied_at"] > baseline[i]["_applied_at"] for i in [0, 1])

# Replay sans réinitialisation : ni valeurs, ni versions, ni timestamps modifiés.
replay = apply_updates()
assert snapshot() == after_update, "Le replay a modifié l'état courant."
assert replay["updated_rows"] == 0
assert replay["inserted_rows"] == 0 and replay["deleted_rows"] == 0

results = {
    "scenario": "012",
    "source_table": SOURCE_TABLE,
    "target_table": TARGET_TABLE,
    "source_rows": 4,
    "source_keys_after_selection": 3,
    "insert_only": insert_only_metrics,
    "first_run": first_run,
    "replay": replay,
    "silver_before": baseline,
    "silver_after": after_update,
    "checks": {"latest_version_kept": True, "control_unchanged": True, "replay_unchanged": True},
}

print("========================================")
print("SCENARIO 012 - CDC UPDATE")
print("========================================")
print(f"UPDATED_ROWS={first_run['updated_rows']}")
print(f"REPLAY_UPDATED_ROWS={replay['updated_rows']}")
print(f"MERGE_DURATION_SECONDS={first_run['duration_seconds']:.3f}")
print(f"REPLAY_DURATION_SECONDS={replay['duration_seconds']:.3f}")
print("========================================")
print(json.dumps(results, default=str, ensure_ascii=False))
