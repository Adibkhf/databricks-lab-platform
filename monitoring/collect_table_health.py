# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    ArrayType,
)


# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "dbx_lab_dev"

DETAIL_TABLE = f"{CATALOG}.monitoring.table_health_delta_detail"
HISTORY_TABLE = f"{CATALOG}.monitoring.table_health_delta_history"


# ============================================================
# 1. TABLES DELTA À ANALYSER
# ============================================================

tables = spark.sql("""
    SELECT
        table_catalog,
        table_schema,
        table_name
    FROM dbx_lab_dev.information_schema.tables
    WHERE table_schema IN (
        'bronze',
        'silver',
        'gold',
        'landing',
        'monitoring'
    )
      AND LOWER(data_source_format) = 'delta'
      AND table_type <> 'VIEW'
      AND table_name NOT IN (
          'table_health_delta_detail',
          'table_health_delta_history'
      )
    ORDER BY table_schema, table_name
""").collect()


# On stocke de petits objets Python.
# Cela évite d'union plusieurs plans Spark issus de detail().
detail_rows = []
history_rows = []


# ============================================================
# 2. COLLECTE
# ============================================================

for t in tables:

    full_name = (
        f"{t.table_catalog}."
        f"{t.table_schema}."
        f"{t.table_name}"
    )

    print(f"Processing: {full_name}")

    try:

        delta_table = DeltaTable.forName(
            spark,
            full_name
        )

        # ----------------------------------------------------
        # DETAIL
        # ----------------------------------------------------

        # first() force immédiatement l'exécution du plan.
        detail = delta_table.detail().first().asDict(
            recursive=True
        )

        detail_rows.append({
            "catalog_name": t.table_catalog,
            "schema_name": t.table_schema,
            "table_name": t.table_name,

            "format": detail.get("format"),
            "location": detail.get("location"),
            "created_at": detail.get("createdAt"),
            "last_modified": detail.get("lastModified"),

            "size_in_bytes": detail.get("sizeInBytes"),
            "num_files": detail.get("numFiles"),

            "partition_columns":
                detail.get("partitionColumns") or [],

            "clustering_columns":
                detail.get("clusteringColumns") or [],
        })


        # ----------------------------------------------------
        # HISTORY
        # ----------------------------------------------------

        # history(20) récupère les 20 dernières versions.
        history = delta_table.history(20).select(
            "version",
            "timestamp",
            "operation",
            "userName"
        ).collect()

        for h in history:

            history_rows.append({
                "catalog_name": t.table_catalog,
                "schema_name": t.table_schema,
                "table_name": t.table_name,

                "version": h["version"],
                "timestamp": h["timestamp"],
                "operation": h["operation"],
                "userName": h["userName"],
            })

        print(f"OK: {full_name}")

    except Exception as e:

        print(
            f"ERROR: {full_name}\n"
            f"{type(e).__name__}: {e}"
        )


# ============================================================
# 3. SCHÉMA DETAIL
# ============================================================

detail_schema = StructType([
    StructField("catalog_name", StringType(), False),
    StructField("schema_name", StringType(), False),
    StructField("table_name", StringType(), False),

    StructField("format", StringType(), True),
    StructField("location", StringType(), True),

    StructField("created_at", TimestampType(), True),
    StructField("last_modified", TimestampType(), True),

    StructField("size_in_bytes", LongType(), True),
    StructField("num_files", LongType(), True),

    StructField(
        "partition_columns",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "clustering_columns",
        ArrayType(StringType()),
        True
    ),
])


# ============================================================
# 4. ÉCRIRE DETAIL
# ============================================================

if detail_rows:

    detail_df = spark.createDataFrame(
        detail_rows,
        schema=detail_schema
    )

    (
        detail_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DETAIL_TABLE)
    )

    print(
        f"DETAIL OK -> {DETAIL_TABLE} "
        f"({len(detail_rows)} tables)"
    )

else:

    print("No DETAIL rows collected.")


# ============================================================
# 5. SCHÉMA HISTORY
# ============================================================

history_schema = StructType([
    StructField("catalog_name", StringType(), False),
    StructField("schema_name", StringType(), False),
    StructField("table_name", StringType(), False),

    StructField("version", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("operation", StringType(), True),
    StructField("userName", StringType(), True),
])


# ============================================================
# 6. ÉCRIRE HISTORY
# ============================================================

if history_rows:

    history_df = spark.createDataFrame(
        history_rows,
        schema=history_schema
    )

    (
        history_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(HISTORY_TABLE)
    )

    print(
        f"HISTORY OK -> {HISTORY_TABLE} "
        f"({len(history_rows)} operations)"
    )

else:

    print("No HISTORY rows collected.")


# ============================================================
# 7. RÉSUMÉ
# ============================================================

print(f"""
========================================
TABLE HEALTH COLLECTOR
========================================
Delta tables discovered : {len(tables)}
DETAIL collected        : {len(detail_rows)}
HISTORY rows collected  : {len(history_rows)}
========================================
""")