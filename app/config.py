import os
import sys
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    java_home: str
    hadoop_home: str
    clients_path: str
    orders_path: str
    app_name: str
    spark_master: str
    spark_driver_host: str
    show_rows: int
    log_level: str

def load_config() -> AppConfig:
    java_home = os.getenv("JAVA_HOME", r"C:\Program Files\Java\jdk-17")
    hadoop_home = os.getenv("HADOOP_HOME", r"C:\hadoop")

    os.environ["JAVA_HOME"] = java_home
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable    

    return AppConfig(
        java_home=java_home,
        hadoop_home=hadoop_home,
        clients_path=os.getenv("CLIENTS_PATH", r"data/clients/data.json"),
        orders_path=os.getenv("ORDERS_PATH", r"data/pedidos/data.json"),
        app_name=os.getenv("APP_NAME", "TesteTecnicoItauPySpark"),
        spark_master=os.getenv("SPARK_MASTER", "local[*]"),
        spark_driver_host=os.getenv("SPARK_DRIVER_HOST", "127.0.0.1"),
        show_rows=int(os.getenv("SHOW_ROWS", "0")), # 0 Exibe todos os registros, para exibir o top 20, use: int(os.getenv("SHOW_ROWS", "20"))
        log_level=os.getenv("LOG_LEVEL", "ERROR"),
    )