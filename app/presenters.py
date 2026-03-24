from pyspark.sql import DataFrame


def show_df(title: str, df: DataFrame, rows: int = 20, truncate: bool = False) -> None:
    print(f"\n=== {title} ===")
    if rows == 0:
        df.show(df.count(), truncate=truncate)
    else:
        df.show(rows, truncate=truncate)