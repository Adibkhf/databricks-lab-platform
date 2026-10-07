# Lot contrôlé exécuté dans Databricks, sans requête Structured Streaming.
import json
import time
from decimal import Decimal
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "dbx_lab_dev.bronze.scenario_014_transactions_cdc"
STATE_TABLE = "dbx_lab_dev.silver.scenario_014_transactions_state"
CURRENT_VIEW = "dbx_lab_dev.silver.scenario_014_transactions_current"
GOLD_TABLE = "dbx_lab_dev.gold.scenario_014_transactions_summary"


def prepare_silver():
    # La table d'état conserve aussi la version des transactions supprimées.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {STATE_TABLE} (
            transaction_id BIGINT, payment_id BIGINT, amount DECIMAL(12, 2),
            sequence_num BIGINT, _is_deleted BOOLEAN, _applied_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {STATE_TABLE} VALUES
            (990001, 1001, 100.00, 1, false, TIMESTAMP'2026-01-01 08:00:00'),
            (990002, 1002, 50.00, 1, false, TIMESTAMP'2026-01-01 08:00:00'),
            (990003, 1003, 30.00, 1, false, TIMESTAMP'2026-01-01 08:00:00')
    """)
    # Exclure les suppressions logiques des lectures métier et du calcul Gold.
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CURRENT_VIEW} AS
        SELECT transaction_id, payment_id, amount, sequence_num, _applied_at
        FROM {STATE_TABLE} WHERE _is_deleted = false
    """)


def rebuild_gold():
    summary = spark.table(CURRENT_VIEW).agg(
        F.count("*").alias("transaction_count"), F.sum("amount").alias("total_amount")
    )
    # Gold est recalculée séparément du MERGE Silver.
    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE)


def state_snapshot():
    # Le jeu du scénario ne dépasse pas quatre clés.
    rows = spark.table(STATE_TABLE).orderBy("transaction_id").limit(5).collect()
    assert len(rows) <= 4
    assert len({row.transaction_id for row in rows}) == len(rows), "Clé transaction dupliquée."
    return [row.asDict() for row in rows]


def gold_snapshot():
    return spark.table(GOLD_TABLE).first().asDict()


def normalize_batch(batch_df):
    # INSERT/UPDATE portent une image complète. DELETE exige seulement clé et version.
    invalid = batch_df.filter(
        "event_id IS NULL OR transaction_id IS NULL OR sequence_num IS NULL "
        "OR sequence_num < 1 OR operation IS NULL OR operation NOT IN ('INSERT', 'UPDATE', 'DELETE') "
        "OR (operation <> 'DELETE' AND (payment_id IS NULL OR amount IS NULL))"
    ).count()
    if invalid:
        raise ValueError("INVALID_EVENT")

    # Une même clé/version ne peut pas décrire deux opérations ou états différents.
    payload = F.struct("operation", "payment_id", "amount")
    conflicts = (
        batch_df.groupBy("transaction_id", "sequence_num")
        .agg(F.countDistinct(payload).alias("states"))
        .filter("states > 1").count()
    )
    if conflicts:
        raise ValueError("AMBIGUOUS_KEY_VERSION")

    # Un identifiant d'événement ne doit pas désigner deux contenus différents.
    event_conflicts = (
        batch_df.groupBy("event_id")
        .agg(F.countDistinct(F.struct("transaction_id", "sequence_num", "operation", "payment_id", "amount")).alias("states"))
        .filter("states > 1").count()
    )
    if event_conflicts:
        raise ValueError("EVENT_ID_CONFLICT")

    # Retirer les copies identiques, puis garder la dernière image complète par clé.
    distinct_events = batch_df.dropDuplicates(["transaction_id", "sequence_num", "operation", "payment_id", "amount"])
    order = Window.partitionBy("transaction_id").orderBy(F.desc("sequence_num"))
    latest = distinct_events.withColumn("version_rank", F.row_number().over(order)).filter("version_rank = 1").drop("version_rank")
    return latest, {
        "source_rows": batch_df.count(),
        "distinct_versions": distinct_events.count(),
        "keys_after_selection": latest.count(),
    }


def merge_metrics(elapsed):
    history = DeltaTable.forName(spark, STATE_TABLE).history(1).first()
    assert history.operation == "MERGE"
    # Un DELETE logique compte comme UPDATE dans les métriques Delta.
    metrics = history.operationMetrics
    return {
        "inserted_rows": int(metrics["numTargetRowsInserted"]),
        "updated_rows": int(metrics["numTargetRowsUpdated"]),
        "deleted_rows": int(metrics["numTargetRowsDeleted"]),
        "duration_seconds": round(elapsed, 3),
    }


def apply_physical_merge(source_df):
    # Reproduction : la suppression physique ne laisse aucune version pour la clé.
    start = time.perf_counter()
    (
        DeltaTable.forName(spark, STATE_TABLE).alias("t")
        .merge(source_df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedDelete(condition="s.operation = 'DELETE'")
        .whenMatchedUpdate(
            condition="s.operation IN ('INSERT', 'UPDATE')",
            set={"payment_id": "s.payment_id", "amount": "s.amount", "sequence_num": "s.sequence_num", "_applied_at": "current_timestamp()"},
        )
        .whenNotMatchedInsert(
            condition="s.operation IN ('INSERT', 'UPDATE')",
            values={"transaction_id": "s.transaction_id", "payment_id": "s.payment_id", "amount": "s.amount", "sequence_num": "s.sequence_num", "_is_deleted": "false", "_applied_at": "current_timestamp()"},
        )
        .execute()
    )
    return merge_metrics(time.perf_counter() - start)


def apply_mixed_merge(source_df):
    # Conserver la version du DELETE pour bloquer une ancienne image reçue ensuite.
    before = state_snapshot()
    start = time.perf_counter()
    (
        DeltaTable.forName(spark, STATE_TABLE).alias("t")
        .merge(source_df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdate(
            # t = Silver, s = CDC. Versions anciennes ou égales : aucune réécriture.
            condition="s.sequence_num > t.sequence_num",
            set={
                # Le DELETE conserve le contenu existant et marque la ligne comme supprimée.
                "payment_id": "CASE WHEN s.operation = 'DELETE' THEN t.payment_id ELSE s.payment_id END",
                "amount": "CASE WHEN s.operation = 'DELETE' THEN t.amount ELSE s.amount END",
                "sequence_num": "s.sequence_num",
                "_is_deleted": "s.operation = 'DELETE'",
                "_applied_at": "current_timestamp()",
            },
        )
        # Un DELETE sans cible crée aussi un marqueur avec sa version.
        .whenNotMatchedInsert(
            values={
                "transaction_id": "s.transaction_id", "payment_id": "s.payment_id", "amount": "s.amount",
                "sequence_num": "s.sequence_num", "_is_deleted": "s.operation = 'DELETE'", "_applied_at": "current_timestamp()",
            },
        )
        .execute()
    )
    metrics = merge_metrics(time.perf_counter() - start)
    after = state_snapshot()
    # Comparer les snapshots pour distinguer les changements métier des compteurs Delta.
    old_state = {row["transaction_id"]: row for row in before}
    changes = {"inserted": 0, "updated": 0, "deleted_logically": 0}
    for row in after:
        previous = old_state.get(row["transaction_id"])
        if not row["_is_deleted"] and (previous is None or previous["_is_deleted"]):
            changes["inserted"] += 1
        elif row["_is_deleted"] and previous is not None and not previous["_is_deleted"]:
            changes["deleted_logically"] += 1
        elif previous is not None and not row["_is_deleted"] and row != previous:
            changes["updated"] += 1
    rebuild_gold()
    return {"delta": metrics, "business_changes": changes}


# Préparer la baseline : trois transactions actives, montant total 180.
prepare_silver()
rebuild_gold()
baseline = state_snapshot()
gold_before = gold_snapshot()
assert gold_before == {"transaction_count": 3, "total_amount": Decimal("180.00")}

# Bronze de test : deux versions pour 990001, un ancien INSERT et un DELETE pour 990002,
# un INSERT dupliqué pour 990004 et un événement de même version pour le témoin 990003.
spark.sql(f"""
    CREATE OR REPLACE TABLE {SOURCE_TABLE} AS
    SELECT event_id, transaction_id, payment_id, CAST(amount AS DECIMAL(12, 2)) AS amount, sequence_num, operation
    FROM VALUES
        ('txn_001_update_v2', 990001L, 1001L, '110.00', 2L, 'UPDATE'),
        ('txn_001_update_v3', 990001L, 1001L, '120.00', 3L, 'UPDATE'),
        ('txn_002_delete_v2', 990002L, CAST(NULL AS BIGINT), CAST(NULL AS STRING), 2L, 'DELETE'),
        ('txn_002_insert_v1', 990002L, 1002L, '50.00', 1L, 'INSERT'),
        ('txn_004_insert_v1', 990004L, 1004L, '40.00', 1L, 'INSERT'),
        ('txn_004_insert_v1', 990004L, 1004L, '40.00', 1L, 'INSERT'),
        ('txn_003_same_v1', 990003L, 1003L, '30.00', 1L, 'UPDATE')
    AS t(event_id, transaction_id, payment_id, amount, sequence_num, operation)
""")
events = spark.table(SOURCE_TABLE)
source_df, preparation = normalize_batch(events)
assert preparation == {"source_rows": 7, "distinct_versions": 6, "keys_after_selection": 4}
# Isoler l'INSERT v1 de 990002 pour le renvoyer volontairement après sa suppression.
old_insert, _ = normalize_batch(events.filter("event_id = 'txn_002_insert_v1'"))

# Le premier lot semble correct ; l'ancien INSERT fait ensuite réapparaître la clé.
physical_first = apply_physical_merge(source_df)
rebuild_gold()
assert gold_snapshot() == {"transaction_count": 3, "total_amount": Decimal("190.00")}
# Sans cible ni version conservée, cet appel recrée 990002 et ajoute 50 à Gold.
physical_old_insert = apply_physical_merge(old_insert)
rebuild_gold()
wrong_state = state_snapshot()
wrong_gold = gold_snapshot()
assert physical_old_insert["inserted_rows"] == 1
assert wrong_gold == {"transaction_count": 4, "total_amount": Decimal("240.00")}

# Restaurer la même baseline pour mesurer la correction dans les mêmes conditions.
prepare_silver()
rebuild_gold()
assert state_snapshot() == baseline and gold_snapshot() == gold_before

# Appliquer le lot corrigé : 990002 reste stockée en v2 avec _is_deleted = true.
first_run = apply_mixed_merge(source_df)
after_mixed = state_snapshot()
gold_after = gold_snapshot()
assert first_run["business_changes"] == {"inserted": 1, "updated": 1, "deleted_logically": 1}
# Métier : 1 INSERT, 1 UPDATE, 1 DELETE logique. Delta : 1 INSERT, 2 UPDATE.
assert first_run["delta"]["inserted_rows"] == 1 and first_run["delta"]["updated_rows"] == 2
assert first_run["delta"]["deleted_rows"] == 0
expected = [(990001, Decimal("120.00"), 3, False), (990002, Decimal("50.00"), 2, True), (990003, Decimal("30.00"), 1, False), (990004, Decimal("40.00"), 1, False)]
assert [(row["transaction_id"], row["amount"], row["sequence_num"], row["_is_deleted"]) for row in after_mixed] == expected
assert after_mixed[2] == baseline[2], "Le témoin ne doit pas changer."
assert gold_after == {"transaction_count": 3, "total_amount": Decimal("190.00")}

# Rejouer le lot puis l'ancien INSERT, sans remise à zéro entre ces passages.
replay = apply_mixed_merge(source_df)
# Même ancien INSERT qu'avant : il trouve maintenant le marqueur v2, donc 1 > 2 est faux.
stale_insert = apply_mixed_merge(old_insert)
for result in [replay, stale_insert]:
    assert result["business_changes"] == {"inserted": 0, "updated": 0, "deleted_logically": 0}
    assert all(result["delta"][name] == 0 for name in ["inserted_rows", "updated_rows", "deleted_rows"])
assert state_snapshot() == after_mixed and gold_snapshot() == gold_after

# Exercer le rejet d'une même clé/version avec deux montants différents.
version_3 = events.filter("event_id = 'txn_001_update_v3'")
conflicting_event = version_3.withColumn("event_id", F.lit("txn_001_conflict_v3")).withColumn("amount", F.lit(Decimal("121.00")).cast("decimal(12, 2)"))
conflict_rejected = False
# Vérifier aussi que le rejet ne crée aucun nouveau commit Silver.
version_before_rejection = DeltaTable.forName(spark, STATE_TABLE).history(1).first().version
try:
    normalize_batch(version_3.unionByName(conflicting_event))
except ValueError as error:
    assert str(error) == "AMBIGUOUS_KEY_VERSION"
    conflict_rejected = True
assert conflict_rejected
assert DeltaTable.forName(spark, STATE_TABLE).history(1).first().version == version_before_rejection
assert state_snapshot() == after_mixed and gold_snapshot() == gold_after

results = {
    "scenario": "014", "execution_mode": "controlled_batch",
    "source_table": SOURCE_TABLE, "state_table": STATE_TABLE, "current_view": CURRENT_VIEW, "gold_table": GOLD_TABLE,
    "preparation": preparation,
    "physical_first": physical_first, "physical_old_insert": physical_old_insert,
    "wrong_state": wrong_state, "wrong_gold": wrong_gold,
    "first_run": first_run, "replay": replay, "stale_insert": stale_insert,
    "state_before": baseline, "state_after": after_mixed,
    "gold_before": gold_before, "gold_after": gold_after,
    "checks": {"control_unchanged": True, "replay_unchanged": True, "stale_insert_blocked": True, "conflict_rejected_before_write": conflict_rejected},
}

print("========================================")
print("SCENARIO 014 - CDC MIXTE")
print("========================================")
print(f"BUSINESS_CHANGES={first_run['business_changes']}")
print(f"DELTA_METRICS={first_run['delta']}")
print(f"GOLD_TOTAL={gold_after['total_amount']}")
print("========================================")
print(json.dumps(results, default=str, ensure_ascii=False))
