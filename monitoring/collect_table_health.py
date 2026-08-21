from functools import reduce

from pyspark.sql import functions as F


# Catalog fixe : le collector ne regarde jamais ailleurs.
CATALOG = "dbx_lab_dev"

SCHEMAS = [
    "bronze",
    "silver",
    "gold",
    "landing",
    "monitoring",
]

DETAIL_TABLE = f"{CATALOG}.monitoring.table_health_delta_detail"
HISTORY_TABLE = f"{CATALOG}.monitoring.table_health_delta_history"


# Récupération uniquement des tables Delta accessibles.
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
""").collect()


detail_frames = []
history_frames = []


for t in tables:

    full_name = f"{t.table_catalog}.{t.table_schema}.{t.table_name}"

    try:
        # IDENTIFIER() évite de construire directement une commande SQL
        # à partir d'un nom de table dynamique.
        detail_df = spark.sql(
            "DESCRIBE DETAIL IDENTIFIER(:table_name)",
            args={"table_name": full_name}
        )

        detail_df = (
            detail_df
            .select(
                F.lit(t.table_catalog).alias("catalog_name"),
                F.lit(t.table_schema).alias("schema_name"),
                F.lit(t.table_name).alias("table_name"),
                F.col("format").alias("format"),
                F.col("location").alias("location"),
                F.col("createdAt").alias("created_at"),
                F.col("lastModified").alias("last_modified"),
                F.col("sizeInBytes").cast("long").alias("size_in_bytes"),
                F.col("numFiles").cast("long").alias("num_files"),
                F.col("partitionColumns").alias("partition_columns"),
                F.col("clusteringColumns").alias("clustering_columns"),
                F.current_timestamp().alias("collected_at")
            )
        )

        detail_frames.append(detail_df)


        # Conserve uniquement les informations utiles au dashboard.
        history_df = spark.sql(
            """
            DESCRIBE HISTORY IDENTIFIER(:table_name)
            LIMIT 20
            """,
            args={"table_name": full_name}
        )

        history_df = (
            history_df
            .select(
                F.lit(t.table_catalog).alias("catalog_name"),
                F.lit(t.table_schema).alias("schema_name"),
                F.lit(t.table_name).alias("table_name"),
                "version",
                "timestamp",
                "operation",
                "userName",
                F.current_timestamp().alias("collected_at")
            )
        )

        history_frames.append(history_df)

    except Exception as e:
        # Une table problématique ne bloque pas tout le collector.
        print(f"SKIPPED {full_name}: {e}")


# Snapshot actuel de DESCRIBE DETAIL.
if detail_frames:
    detail_result = reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=True
        ),
        detail_frames
    )

    (
        detail_result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DETAIL_TABLE)
    )


# 20 dernières opérations par table.
if history_frames:
    history_result = reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=True
        ),
        history_frames
    )

    (
        history_result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(HISTORY_TABLE)
    )


print(f"Collected {len(tables)} Delta tables.")