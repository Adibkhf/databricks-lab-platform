import time
from delta.tables import DeltaTable

SOURCE_TABLE = "dbx_lab_dev.bronze.scenario_011_orders_cdc"
TARGET_TABLE = "dbx_lab_dev.silver.scenario_011_orders_current"

source_df = (
    spark.table(SOURCE_TABLE)
    .filter("operation = 'INSERT'")
)

target = DeltaTable.forName(spark, TARGET_TABLE)

rows_before = spark.table(TARGET_TABLE).count()

start = time.perf_counter()

# INSERT uniquement si order_id n'existe pas déjà dans Silver.
(
    target.alias("t")
    .merge(
        source_df.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenNotMatchedInsert(
        values={
            "order_id": "s.order_id",
            "customer_id": "s.customer_id",
            "order_date": "s.order_date",
            "status": "s.status"
        }
    )
    .execute()
)

elapsed = time.perf_counter() - start

rows_after = spark.table(TARGET_TABLE).count()

print("========================================")
print("SCENARIO 011 - CDC INSERT")
print("========================================")
print(f"ROWS_BEFORE={rows_before}")
print(f"ROWS_AFTER={rows_after}")
print(f"INSERTED_ROWS={rows_after - rows_before}")
print(f"MERGE_DURATION_SECONDS={elapsed:.2f}")
print("========================================")