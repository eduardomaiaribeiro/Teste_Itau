from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def build_customer_aggregation(df_valid_orders: DataFrame, df_clients: DataFrame) -> DataFrame:
    orders_by_client = (
        df_valid_orders
        .groupBy("client_id")
        .agg(
            F.count("*").alias("qtd_pedidos"),
            F.sum("value").cast(DecimalType(11, 2)).alias("valor_total")
        )
    )

    return (
        df_clients
        .join(
            F.broadcast(orders_by_client),
            df_clients.id == orders_by_client.client_id,
            "left"
        )
        .select(
            df_clients.name.alias("nome_cliente"),
            F.coalesce(F.col("qtd_pedidos"), F.lit(0)).alias("qtd_pedidos"),
            F.coalesce(
                F.col("valor_total"),
                F.lit(0).cast(DecimalType(11, 2))
            ).alias("valor_total")
        )
        .orderBy(F.col("valor_total").desc())
    )