import logging

from pyspark.sql import Window,  functions as F
from src.common import config
from src.common.spark_session import get_spark


def clean_crm_cust_info(df):
    result = df.groupBy(F.col("cst_id")).count().alias("Count")
    return result


if __name__ == "__main__":
    spark = get_spark("Test Silver")
    df = spark.read.parquet(str(config.BRONZE_DIR/"crm_cust_info"))
    clean_crm_cust_info(df).show(10, truncate=False)
    spark.stop()