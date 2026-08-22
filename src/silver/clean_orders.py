from pyspark.sql import functions as F

SOURCE_TABLE = "dbx_lab_dev.bronze.orders_raw"
TARGET_TABLE = "dbx_lab_dev.silver.orders_clean"
QUARANTINE_TABLE = "dbx_lab_dev.silver.orders_quarantine"

# Charge les données Bronze.
bronze_df = spark.table(SOURCE_TABLE)

# Normalise les types et valeurs.
normalized_df = (
    bronze_df
    # try_cast retourne NULL si la valeur n'est pas convertible.
    .withColumn("order_id", F.col("order_id").try_cast("long"))
    .withColumn("customer_id", F.col("customer_id").try_cast("long"))
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("ingestion_date", F.to_date(F.col("ingestion_date")))
)

# Lignes valides.
valid_df = (
    normalized_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("order_date").isNotNull())
    .filter(F.col("status").isin("created", "paid", "shipped", "cancelled"))
    .filter(F.col("_rescued_data").isNull())
)

# Lignes invalides envoyées en quarantaine.
invalid_df = (
    normalized_df
    .filter(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_date").isNull()
        | (~F.col("status").isin("created", "paid", "shipped", "cancelled"))
        | F.col("_rescued_data").isNotNull()
    )
)

# Écrit les lignes valides.
valid_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

# Écrit les rejets pour investigation.
invalid_df.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TABLE)

print(f"Silver valid rows: {valid_df.count()}")
print(f"Quarantined rows: {invalid_df.count()}")