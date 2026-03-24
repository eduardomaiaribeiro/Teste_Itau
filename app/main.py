from app.config import load_config
from app.logger import get_logger


def main() -> None:
    config = load_config()  # configura JAVA_HOME, HADOOP_HOME, PATH antes do pyspark
    logger = get_logger()

    # imports tardios, depois do ambiente configurado
    from app.session import create_spark_session
    from app.readers import read_clients, read_orders
    from app.quality import build_quality_report, build_valid_orders
    from app.aggregation import build_customer_aggregation
    from app.statistics import build_statistics, build_above_average, build_trimmed_range
    from app.presenters import show_df

    spark = None

    try:
        logger.info("Iniciando processamento")
        spark = create_spark_session(config)
        logger.info("Spark criado com sucesso")

        logger.info("Iniciando leitura dos Clientes")
        df_clients = read_clients(spark, config.clients_path)
        logger.info("Clientes lidos com sucesso")
        
        logger.info("Iniciando leitura dos Pedidos")
        df_orders = read_orders(spark, config.orders_path)
        logger.info("Pedidos lidos com sucesso")

        logger.info("Iniciando Data Quality")
        quality_report_df = build_quality_report(df_orders, df_clients)
        logger.info("Data Quality concluído com sucesso")

        logger.info("Iniciando construção de pedidos válidos")
        valid_orders_df = build_valid_orders(df_orders, df_clients)
        logger.info("Pedidos válidos construídos com sucesso")

        logger.info("Iniciando agregação por cliente")
        customer_agg_df = build_customer_aggregation(valid_orders_df, df_clients)
        logger.info("Agregação por cliente concluída com sucesso")

        customer_agg_df.cache()

        logger.info("Iniciando construção de estatísticas")
        stats_df = build_statistics(customer_agg_df)
        logger.info("Estatísticas construídas com sucesso")
        stats_row = stats_df.first()

        logger.info("Iniciando construção de clientes acima da média")
        above_average_df = build_above_average(customer_agg_df, stats_row["Média aritmética"])
        logger.info("Clientes acima da média construídos com sucesso")

        logger.info("Iniciando construção de clientes entre percentis 10 e 90")
        trimmed_df = build_trimmed_range(
            customer_agg_df,
            stats_row["Percentil 10 (10% inferiores)"],
            stats_row["Percentil 90 (90% inferiores)"],
        )
        logger.info("Clientes entre percentis 10 e 90 construídos com sucesso")

        show_df("REQUISITO 1 - DATA QUALITY", quality_report_df, rows=config.show_rows)
        show_df("REQUISITO 2 - AGREGAÇÃO POR CLIENTE", customer_agg_df, rows=config.show_rows)
        show_df("REQUISITO 3 - ESTATÍSTICAS", stats_df, rows=config.show_rows)
        show_df("REQUISITO 4 - CLIENTES ACIMA DA MÉDIA", above_average_df, rows=0)
        show_df("REQUISITO 5 - CLIENTES ENTRE P10 E P90", trimmed_df, rows=0)

        logger.info("Processamento concluído com sucesso")

    except Exception as exc:
        logger.exception("Erro durante o processamento: %s", exc)
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark finalizado")


if __name__ == "__main__":
    main()