import json
import time
from decimal import Decimal
from delta.tables import DeltaTable
from pyspark.sql import functions as F

SOURCE_TABLE = "dbx_lab_dev.bronze.scenario_013_payments_cdc"
TARGET_TABLE = "dbx_lab_dev.silver.scenario_013_payments_current"
GOLD_TABLE = "dbx_lab_dev.gold.scenario_013_payments_daily"
AUDIT_TABLE = "dbx_lab_dev.bronze.scenario_013_delete_audit"


def prepare_silver():
    # Trois paiements fictifs, uniquement dans la cible réservée au scénario.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TARGET_TABLE} AS
        SELECT payment_id, DATE'2026-10-06' AS payment_date,
               CAST(amount AS DECIMAL(12, 2)) AS amount,
               'accepted' AS status, sequence_num,
               TIMESTAMP'2026-01-01 08:00:00' AS _applied_at
        FROM VALUES
            (980001L, '100.00', 1L),
            (980002L, '50.00', 3L),
            (980003L, '30.00', 1L)
        AS t(payment_id, amount, sequence_num)
    """)


def rebuild_gold():
    # Le DELETE Silver ne recalcule pas les agrégats Gold automatiquement.
    daily = (
        spark.table(TARGET_TABLE)
        .groupBy("payment_date", "status")
        .agg(F.count("*").alias("payment_count"), F.sum("amount").alias("payment_total"))
    )
    daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE)


def silver_snapshot():
    # La collecte est limitée au petit jeu de trois paiements du test.
    rows = spark.table(TARGET_TABLE).orderBy("payment_id").limit(4).collect()
    assert len(rows) <= 3
    assert len({row.payment_id for row in rows}) == len(rows), "Clé paiement dupliquée."
    return [row.asDict() for row in rows]


def gold_snapshot():
    rows = spark.table(GOLD_TABLE).orderBy("payment_date", "status").limit(2).collect()
    assert len(rows) == 1, "Un seul groupe est attendu dans ce jeu de test."
    return [row.asDict() for row in rows]


def delete_metrics(elapsed):
    history = DeltaTable.forName(spark, TARGET_TABLE).history(1).first()
    assert history.operation == "MERGE"
    metrics = history.operationMetrics
    return {
        "deleted_rows": int(metrics.get("numTargetRowsDeleted", 0)),
        "updated_rows": int(metrics.get("numTargetRowsUpdated", 0)),
        "inserted_rows": int(metrics.get("numTargetRowsInserted", 0)),
        "duration_seconds": round(elapsed, 3),
    }


prepare_silver()
rebuild_gold()
baseline = silver_snapshot()
gold_before = gold_snapshot()
assert gold_before[0]["payment_count"] == 3
assert gold_before[0]["payment_total"] == Decimal("180.00")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SOURCE_TABLE} AS
    SELECT * FROM VALUES
        ('delete_001', 980001L, 2L, 'DELETE'),
        ('delete_002', 980002L, 2L, 'DELETE')
    AS t(event_id, payment_id, sequence_num, operation)
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {AUDIT_TABLE} (
        event_id STRING, payment_id BIGINT, sequence_num BIGINT,
        pass_name STRING, outcome STRING, _audited_at TIMESTAMP
    ) USING DELTA
""")

source_df = spark.table(SOURCE_TABLE)
invalid_rows = source_df.filter(
    "event_id IS NULL OR payment_id IS NULL OR sequence_num IS NULL "
    "OR sequence_num < 1 OR operation IS NULL OR operation <> 'DELETE'"
).count()
assert invalid_rows == 0, "Le lot doit contenir des DELETE valides."
assert source_df.groupBy("payment_id").count().filter("count > 1").count() == 0, "Plusieurs événements pour un paiement."
assert source_df.groupBy("event_id").count().filter("count > 1").count() == 0, "Identifiant d'événement dupliqué."

# Reproduction : sans comparaison des versions, le paiement v3 est supprimé à tort.
start = time.perf_counter()
(
    DeltaTable.forName(spark, TARGET_TABLE).alias("t")
    .merge(source_df.alias("s"), "t.payment_id = s.payment_id")
    .whenMatchedDelete(condition="s.operation = 'DELETE'")
    .execute()
)
unconditional_delete = delete_metrics(time.perf_counter() - start)
rebuild_gold()
wrong_silver = silver_snapshot()
wrong_gold = gold_snapshot()
assert unconditional_delete["deleted_rows"] == 2
assert wrong_silver == [baseline[2]]
assert wrong_gold[0]["payment_total"] == Decimal("30.00")

# Revenir à la même baseline avant de mesurer la correction.
prepare_silver()
rebuild_gold()
assert silver_snapshot() == baseline and gold_snapshot() == gold_before


def apply_deletes(pass_name):
    before = silver_snapshot()
    current_versions = {row["payment_id"]: row["sequence_num"] for row in before}
    events = source_df.orderBy("event_id").limit(3).collect()
    assert len(events) == 2
    audit_rows = []
    expected_deleted = set()

    for event in events:
        current_version = current_versions.get(event.payment_id)
        if current_version is None:
            outcome = "ALREADY_ABSENT"
        elif event.sequence_num > current_version:
            outcome = "DELETED"
            expected_deleted.add(event.payment_id)
        else:
            outcome = "IGNORED_VERSION"
        audit_rows.append((event.event_id, event.payment_id, event.sequence_num, pass_name, outcome))

    start = time.perf_counter()
    (
        DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(source_df.alias("s"), "t.payment_id = s.payment_id")
        .whenMatchedDelete(condition="s.operation = 'DELETE' AND s.sequence_num > t.sequence_num")
        .execute()
    )
    metrics = delete_metrics(time.perf_counter() - start)

    # Vérifier les suppressions et les survivants avant d'écrire l'audit.
    assert metrics["deleted_rows"] == len(expected_deleted)
    assert metrics["updated_rows"] == 0 and metrics["inserted_rows"] == 0
    assert silver_snapshot() == [row for row in before if row["payment_id"] not in expected_deleted]

    audit = spark.createDataFrame(audit_rows, "event_id STRING, payment_id LONG, sequence_num LONG, pass_name STRING, outcome STRING")
    audit.withColumn("_audited_at", F.current_timestamp()).write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    rebuild_gold()
    return metrics


first_run = apply_deletes("first_run")
after_delete = silver_snapshot()
gold_after = gold_snapshot()
assert first_run["deleted_rows"] == 1
assert after_delete == baseline[1:], "Le paiement v3 et le témoin doivent rester identiques."
assert gold_after[0]["payment_count"] == 2
assert gold_after[0]["payment_total"] == Decimal("80.00")

# Le replay n'est pas précédé d'une réinitialisation de Silver ou de l'audit.
replay = apply_deletes("replay")
assert replay["deleted_rows"] == 0
assert silver_snapshot() == after_delete and gold_snapshot() == gold_after
audit_rows = spark.table(AUDIT_TABLE).orderBy("pass_name", "event_id").limit(5).collect()
assert len(audit_rows) == 4
expected_outcomes = {
    ("first_run", "delete_001"): "DELETED",
    ("first_run", "delete_002"): "IGNORED_VERSION",
    ("replay", "delete_001"): "ALREADY_ABSENT",
    ("replay", "delete_002"): "IGNORED_VERSION",
}
assert {(row.pass_name, row.event_id): row.outcome for row in audit_rows} == expected_outcomes

results = {
    "scenario": "013",
    "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
    "gold_table": GOLD_TABLE, "audit_table": AUDIT_TABLE,
    "unconditional_delete": unconditional_delete,
    "first_run": first_run, "replay": replay,
    "silver_before": baseline, "silver_wrong_delete": wrong_silver, "silver_after": after_delete,
    "gold_before": gold_before, "gold_wrong_delete": wrong_gold, "gold_after": gold_after,
    "audit": [row.asDict() for row in audit_rows],
    "checks": {"stale_delete_ignored": True, "survivors_unchanged": True, "gold_reconciled": True, "replay_unchanged": True},
}

print("========================================")
print("SCENARIO 013 - CDC DELETE")
print("========================================")
print(f"DELETED_ROWS={first_run['deleted_rows']}")
print(f"REPLAY_DELETED_ROWS={replay['deleted_rows']}")
print(f"PAYMENTS_AFTER={gold_after[0]['payment_count']}")
print(f"PAYMENT_TOTAL_AFTER={gold_after[0]['payment_total']}")
print("========================================")
print(json.dumps(results, default=str, ensure_ascii=False))
