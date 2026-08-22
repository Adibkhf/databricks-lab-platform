from pathlib import Path
import runpy

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameWriter


@pytest.fixture(scope="session")
def spark():
    # Spark local uniquement pour les tests.
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-clean-orders")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_clean_orders_valid_and_invalid_rows(spark, monkeypatch):
    # Données simulant la Bronze : une ligne valide et deux invalides.
    input_df = spark.createDataFrame(
        [
            ("1", "100", "2026-08-20", " PAID ", "2026-08-21", None),
            ("2", "101", "2026-08-20", "UNKNOWN", "2026-08-21", None),
            ("BAD_ID", "102", "2026-08-20", "created", "2026-08-21", None),
        ],
        """
        order_id STRING,
        customer_id STRING,
        order_date STRING,
        status STRING,
        ingestion_date STRING,
        _rescued_data STRING
        """,
    )

    # Remplace spark.table() afin de ne pas accéder au vrai catalog Databricks.
    monkeypatch.setattr(
        spark,
        "table",
        lambda table_name: input_df,
    )

    # Neutralise les écritures Delta/saveAsTable pendant le test.
    monkeypatch.setattr(
        DataFrameWriter,
        "saveAsTable",
        lambda self, table_name: None,
    )

    # Exécute le vrai fichier clean_orders.py avec notre Spark de test.
    script_path = (
        Path(__file__).parents[1]
        / "src"
        / "silver"
        / "clean_orders.py"
    )

    result = runpy.run_path(
        str(script_path),
        init_globals={"spark": spark},
    )

    # Récupère les DataFrames réellement produits par le script.
    valid_df = result["valid_df"]
    invalid_df = result["invalid_df"]

    # Une seule ligne doit passer les contrôles.
    assert valid_df.count() == 99

    # Deux lignes doivent partir en quarantaine.
    assert invalid_df.count() == 2

    # Vérifie également la normalisation du statut.
    valid_row = valid_df.collect()[0]

    assert valid_row.order_id == 1
    assert valid_row.customer_id == 100
    assert valid_row.status == "paid"