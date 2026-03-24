from pyspark.sql import DataFrame, SparkSession
from app.schemas import CLIENTS_SCHEMA, ORDERS_SCHEMA


def read_clients(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(CLIENTS_SCHEMA).json(path)


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(ORDERS_SCHEMA).json(path)