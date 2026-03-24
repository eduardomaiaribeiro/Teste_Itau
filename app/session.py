from pyspark.sql import SparkSession
from app.config import AppConfig


def create_spark_session(config: AppConfig) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(config.app_name)
        .master(config.spark_master)
        .config("spark.driver.host", config.spark_driver_host)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.log_level)
    return spark