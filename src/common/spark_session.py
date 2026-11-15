import os
import sys

from src.common import config

# Windows only: tell Spark where winutils lives
if os.name == "nt" and os.path.exists(r"C:\hadoop\bin"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def get_spark(app_name="dwh_pipeline"):
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages",
        "org.postgresql:postgresql:42.7.4,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0")   
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", os.getenv("HIVE_METASTORE_URI", "thrift://host.docker.internal:9083"))
        .config("spark.sql.catalog.lakehouse.warehouse", str(config.WAREHOUSE_DIR))     
        .getOrCreate()
    )