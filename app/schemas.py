from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType


CLIENTS_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("client_id", LongType(), True),
    StructField("value", DecimalType(10, 2), True),
])