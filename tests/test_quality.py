from decimal import Decimal
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType

from app.quality import (
    build_quality_report,
    build_valid_orders,
)


def test_quality_report_and_valid_orders(spark):
    clients_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ])

    orders_schema = StructType([
        StructField("id", LongType(), True),
        StructField("client_id", LongType(), True),
        StructField("value", DecimalType(10, 2), True),
    ])

    clients_data = [
        (1, "Ana"),
        (2, "Bruno"),
    ]

    orders_data = [
        (100, 1, Decimal("10.00")),   # válido
        (101, 99, Decimal("20.00")),  # órfão
        (102, 1, Decimal("0.00")),    # inválido valor
        (103, None, Decimal("15.00")),# client_id nulo
        (104, 2, None),               # value nulo
        (105, 2, Decimal("30.00")),   # duplicado 1
        (105, 2, Decimal("30.00")),   # duplicado 2
        (None, 1, Decimal("9.00")),   # id nulo
    ]

    df_clients = spark.createDataFrame(clients_data, schema=clients_schema)
    df_orders = spark.createDataFrame(orders_data, schema=orders_schema)

    quality_df = build_quality_report(df_orders, df_clients)
    valid_orders_df = build_valid_orders(df_orders, df_clients)

    quality_rows = {row["id"]: row["motivo"] for row in quality_df.collect()}

    assert quality_rows[101] == "cliente não encontrado"
    assert quality_rows[102] == "value menor ou igual a zero"
    assert quality_rows[103] == "client_id nulo"
    assert quality_rows[104] == "value nulo"
    assert quality_rows[105] == "pedido duplicado"
    assert quality_rows[None] == "id nulo"

    valid_ids = [row["id"] for row in valid_orders_df.collect()]
    assert valid_ids == [100]