from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def build_statistics(df_customer_agg: DataFrame) -> DataFrame:
    return (
        df_customer_agg
        .select(
            F.avg("valor_total").cast(DecimalType(11, 2)).alias("Média aritmética"),
            F.percentile_approx("valor_total", 0.5).cast(DecimalType(11, 2)).alias("Mediana"),
            F.percentile_approx("valor_total", 0.1).cast(DecimalType(11, 2)).alias("Percentil 10 (10% inferiores)"),
            F.percentile_approx("valor_total", 0.9).cast(DecimalType(11, 2)).alias("Percentil 90 (90% inferiores)"),
        )
    )


def build_above_average(df_customer_agg: DataFrame, media) -> DataFrame:
    return (
        df_customer_agg
        .filter(F.col("valor_total") > F.lit(media))
        .orderBy(F.col("valor_total").desc(), F.col("nome_cliente").asc())
    )


def build_trimmed_range(df_customer_agg: DataFrame, p10, p90) -> DataFrame:
    return (
        df_customer_agg
        .filter(
            (F.col("valor_total") >= F.lit(p10)) &
            (F.col("valor_total") <= F.lit(p90))
        )
        .orderBy(F.col("valor_total").desc(), F.col("nome_cliente").asc())
    )