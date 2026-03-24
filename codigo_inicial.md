
## Este foi o meu primeiro código, tudo em uma única função main, sem segregação de responsabilidades
## A idéia é mostrar como eu evolui para o código final

## A parte referente a DATA QUALITY não estava atendendo ao requisito de retornar os múltiplos motivos, apenas um
## A parte referente a agregação estava atendendo ao requisito, mas não estava formatando os valores em DecimalType(11,2)
## A parte referente a estatísticas estava atendendo ao requisito, mas não estava formatando os valores em DecimalType(11,2)
## A parte referente a filtros estava atendendo ao requisito, mas não estava formatando os valores em DecimalType(11,2)
## A parte referente a filtros estava atendendo ao requisito, mas não estava formatando os valores em DecimalType(11,2)

import os
import sys

# --- 1. CONFIGURAÇÃO DE AMBIENTE ---
JAVA_HOME = r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
HADOOP_HOME = r"C:\hadoop"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType

def main():
    try:
        print(f"--- Iniciando Processamento de Dados ---")
        
        spark = SparkSession.builder \
            .appName("TesteEduardoFinal") \
            .master("local[*]") \
            .config("spark.driver.host", "127.0.0.1") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        print("✅ Spark Online!\n")

        # Schemas
        schema_clients = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True)
        ])
        schema_pedidos = StructType([
            StructField("id", LongType(), True),
            StructField("client_id", LongType(), True),
            StructField("value", DecimalType(10, 2), True)
        ])

        # Leitura
        df_clients = spark.read.json("C:\\Teste_Eduardo\\data\\clients\\data.json", schema=schema_clients)
        df_pedidos = spark.read.json("C:\\Teste_Eduardo\\data\\pedidos\\data.json", schema=schema_pedidos)

        # 1. DATA QUALITY
        print("REQUISITO 1: Data Quality")
        # Identificar pedidos órfãos
        df_orphan = df_pedidos.join(df_clients, df_pedidos.client_id == df_clients.id, "left_anti") \
            .select(df_pedidos.id, F.lit("Cliente não encontrado").alias("motivo"))

        # Identificar pedidos duplicados
        df_duplicados = df_pedidos.groupBy("id").count().filter(F.col("count") > 1) \
            .select(F.col("id"), F.lit("Pedido duplicado").alias("motivo"), F.col("count").alias("quantidade"), F.col("client_id"))

        # Exibir report de qualidade (órfãos + duplicados)
        df_orphan.unionByName(df_duplicados).show(truncate=False)

        # Remover as duplicidades dos pedidos para não afetar as agregações seguintes
        df_pedidos = df_pedidos.dropDuplicates(["id"])

        # 2. AGREGAÇÃO (Correção da Ambiguidade aqui)
        print("REQUISITO 2: Agregação por Cliente")
        df_agg = df_pedidos.join(F.broadcast(df_clients), df_pedidos.client_id == df_clients.id) \
            .groupBy(df_clients.id.alias("c_id"), "name") \
            .agg(
                F.count(df_pedidos.id).alias("qtd_pedidos"),
                F.sum("value").cast("decimal(11,2)").alias("valor_total")
            ).orderBy(F.col("valor_total").desc())
        
        df_agg.cache()
        df_agg.show(15)

        # 3. ESTATÍSTICAS
        print("REQUISITO 3: Estatísticas")
        stats = df_agg.select(
            F.avg("valor_total").alias("media"),
            F.percentile_approx("valor_total", 0.5).alias("mediana"),
            F.percentile_approx("valor_total", 0.1).alias("p10"),
            F.percentile_approx("valor_total", 0.9).alias("p90")
        ).collect()[0]

        metrics = [
            ("Média", float(stats['media'])),
            ("Mediana", float(stats['mediana'])),
            ("P10", float(stats['p10'])),
            ("P90", float(stats['p90']))
        ]
        spark.createDataFrame(metrics, ["Metrica", "Valor"]).show()

        # 4 & 5. FILTROS
        print(f"REQUISITO 4: Acima da Média ({stats['media']:.2f})")
        df_agg.filter(F.col("valor_total") > stats['media']).orderBy("valor_total").show(10)

        print(f"REQUISITO 5: Média Truncada (Entre {stats['p10']:.2f} e {stats['p90']:.2f})")
        df_agg.filter((F.col("valor_total") >= stats['p10']) & (F.col("valor_total") <= stats['p90'])) \
            .orderBy("valor_total").show(10)

    except Exception as e:
        print(f"Erro: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()