from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_null_order_id(df_orders: DataFrame) -> DataFrame:
    return (
        df_orders
        .filter(F.col("id").isNull())
        .select(F.col("id"), F.lit("id nulo").alias("motivo"))
    )


def check_null_client_id(df_orders: DataFrame) -> DataFrame:
    return (
        df_orders
        .filter(F.col("client_id").isNull())
        .select(F.col("id"), F.lit("client_id nulo").alias("motivo"))
    )


def check_null_value(df_orders: DataFrame) -> DataFrame:
    return (
        df_orders
        .filter(F.col("value").isNull())
        .select(F.col("id"), F.lit("value nulo").alias("motivo"))
    )


def check_non_positive_value(df_orders: DataFrame) -> DataFrame:
    return (
        df_orders
        .filter(F.col("value").isNotNull() & (F.col("value") <= F.lit(0)))
        .select(F.col("id"), F.lit("value menor ou igual a zero").alias("motivo"))
    )

def check_duplicate_order_id(df_orders: DataFrame) -> DataFrame:
    duplicate_ids = (
        df_orders
        .filter(F.col("id").isNotNull())
        .groupBy("id")
        .agg(F.countDistinct("client_id").alias("count"))
        .filter(F.col("count") > 1)
        .select("id")
    )

    return (
        duplicate_ids
        .select(F.col("id"), F.lit("pedido duplicado com client_id diferente").alias("motivo"))
    )

def check_duplicate_order_id_client_id(df_orders: DataFrame) -> DataFrame:
    duplicate_ids = (
        df_orders
        .filter(F.col("id").isNotNull())
        .groupBy("id", "client_id")
        .count()
        .filter(F.col("count") > 1)
        .select("id", "client_id")
    )

    return (
        duplicate_ids
        .select(F.col("id"), F.lit("pedido duplicado com o mesmo client_id").alias("motivo"))
    )


def check_orphan_client(df_orders: DataFrame, df_clients: DataFrame) -> DataFrame:
    clients_ref = df_clients.select(F.col("id").alias("client_ref_id"))

    return (
        df_orders
        .filter(F.col("client_id").isNotNull())
        .join(
            F.broadcast(clients_ref),
            F.col("client_id") == F.col("client_ref_id"),
            "left_anti"
        )
        .select(F.col("id"), F.lit("cliente não encontrado").alias("motivo"))
    )


def build_quality_report(df_orders: DataFrame, df_clients: DataFrame) -> DataFrame:
    checks = [
        check_null_order_id(df_orders),
        check_null_client_id(df_orders),
        check_null_value(df_orders),
        check_non_positive_value(df_orders),
        check_duplicate_order_id(df_orders),
        check_duplicate_order_id_client_id(df_orders),
        check_orphan_client(df_orders, df_clients),
    ]

    unified = reduce(lambda acc, nxt: acc.unionByName(nxt), checks)

    return (
        unified
        .groupBy("id")
        .agg(
            F.concat_ws("; ", F.sort_array(F.collect_set("motivo"))).alias("motivo")
        )
        .orderBy(F.col("id").asc_nulls_last())
    )


def build_valid_orders(df_orders: DataFrame, df_clients: DataFrame) -> DataFrame:
    unique_ids = (
        df_orders
        .filter(F.col("id").isNotNull())
        .groupBy("id")
        .count()
        .filter(F.col("count") == 1)
        .select("id")
    )

    valid_client_ids = df_clients.select(F.col("id").alias("valid_client_id"))

    return (
        df_orders
        .filter(
            F.col("id").isNotNull() &
            F.col("client_id").isNotNull() &
            F.col("value").isNotNull() &
            (F.col("value") > F.lit(0))
        )
        .join(unique_ids, on="id", how="inner")
        .join(
            F.broadcast(valid_client_ids),
            F.col("client_id") == F.col("valid_client_id"),
            "inner"
        )
        .select("id", "client_id", "value")
    )