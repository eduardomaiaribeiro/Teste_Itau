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
        (101, 99, Decimal("20.00")),  # órfão (cliente 99 não existe)
        (102, 1, Decimal("0.00")),    # inválido valor zerado
        (103, None, Decimal("15.00")),# client_id nulo
        (104, 2, None),               # value nulo
        (105, 1, Decimal("30.00")),   # duplicado 1 (cliente 1)
        (105, 2, Decimal("30.00")),   # duplicado 2 (cliente 2 -> clientes diferentes!)
        (106, 2, Decimal("-5.00")),   # valor negativo
        (None, 1, Decimal("9.00")),   # id nulo
        (107, None, Decimal("-1.00")) # múltiplos erros (client_id nulo e valor negativo)
    ]

    df_clients = spark.createDataFrame(clients_data, schema=clients_schema)
    df_orders = spark.createDataFrame(orders_data, schema=orders_schema)

    quality_df = build_quality_report(df_orders, df_clients)
    valid_orders_df = build_valid_orders(df_orders, df_clients)

    # Coleta em dicionário: as chaves originais podem ser nulas (None), então lide com isso
    quality_rows = {row["id"]: row["motivo"] for row in quality_df.collect()}

    # Verificações individuais de motivos de falha associados a cada ID
    assert "cliente não encontrado" in quality_rows[101]
    assert "value menor ou igual a zero" in quality_rows[102]
    assert "client_id nulo" in quality_rows[103]
    assert "value nulo" in quality_rows[104]
    assert "pedido duplicado" in quality_rows[105]
    assert "value menor ou igual a zero" in quality_rows[106]
    assert "id nulo" in quality_rows[None]

    # Verificação de MÚLTIPLOS ERROS (Deverá ser concatenado com '; ')
    assert "client_id nulo" in quality_rows[107]
    assert "value menor ou igual a zero" in quality_rows[107]
    assert ";" in quality_rows[107]

    # Apenas o pedido 100 pode ser considerado válido e constar na camada Gold
    valid_ids = [row["id"] for row in valid_orders_df.collect()]
    assert valid_ids == [100]