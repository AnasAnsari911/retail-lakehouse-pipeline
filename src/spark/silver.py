import logging

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src.common import config
from src.common.spark_session import get_spark


def clean_crm_cust_info(df):

    df = df.filter(F.col("cst_id").isNotNull())

    window_spec = (
        Window
        .partitionBy("cst_id")
        .orderBy(F.col("cst_create_date").desc())
    )

    df = (
        df
        .withColumn(
            "row_number",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    df = (
        df
        .withColumn(
            "cst_firstname",
            F.trim(F.col("cst_firstname"))
        )
        .withColumn(
            "cst_lastname",
            F.trim(F.col("cst_firstname"))
        )
    )

    df = df.withColumn(
        "cst_marital_status",
        F.when(
            F.upper(F.trim(F.col("cst_marital_status"))) == "S",
            "Single"
        )
        .when(
            F.upper(F.trim(F.col("cst_marital_status"))) == "M",
            "Married"
        )
        .otherwise("n/a")
    )

    df = df.withColumn(
        "cst_gndr",
        F.when(
            F.upper(F.trim(F.col("cst_gndr"))) == "M",
            "Male"
        )
        .when(
            F.upper(F.trim(F.col("cst_gndr"))) == "F",
            "Female"
        )
        .otherwise("n/a")
    )

    df = df.select(
        "cst_id",
        "cst_key",
        "cst_firstname",
        "cst_lastname",
        "cst_marital_status",
        "cst_gndr",
        "cst_create_date"
    )

    df = df.withColumn("dwh_load_date", F.current_timestamp())

    return df


def clean_crm_prd_info(df):

    df = df.withColumn(
            "cat_id",
            F.regexp_replace(
                F.substring(F.col("prd_key"), 1,5),
                "-",
                "_"
            )
        )   

    df = df.withColumn("prd_start_dt", F.col("prd_start_dt").cast(DateType()))
    
    window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    
    df = df.withColumn(
        "prd_end_dt",
        F.date_sub(
            F.lead("prd_start_dt", 1).over(window_spec),
                1
        )
    )

    df = df.withColumn("prd_key",F.substring("prd_key", 7, 100))

    df = df.fillna(0,subset=["prd_cost"])

    df = df.withColumn(
        "prd_line",
        F.upper(F.trim(F.col("prd_line")))
    )

    df = df.withColumn(
            "prd_line",
            F.when(F.col("prd_line") == "M", "Mountain")
            .when(F.col("prd_line") == "S", "Other Sales")
            .when(F.col("prd_line") == "T", "Touring")
            .when(F.col("prd_line") == "R", "Road")
            .otherwise("n/a")
    )

    df = df.select(
        "prd_id", 
        "cat_id", 
        "prd_key", 
        "prd_nm", 
        "prd_cost", 
        "prd_line", 
        "prd_start_dt", 
        "prd_end_dt"
    )

    df = df.withColumn("dwh_load_date", F.current_timestamp())

    return df




def load_silver_table(spark, table_name, clean_func):
    df = spark.read.parquet(str(config.BRONZE_DIR / table_name))
    df = clean_func(df)
    df.write.mode("overwrite").parquet(str(config.SILVER_DIR / table_name))
    return spark.read.parquet(str(config.SILVER_DIR / table_name)).count()





if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format= "%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    spark = get_spark("silver_load")
    try :
        count = load_silver_table(spark, "crm_prd_info", clean_crm_prd_info)
        logger.info(f"crm_prd_info : {count}")
        spark.read.parquet(str(config.SILVER_DIR / "crm_prd_info")).show(10, truncate=False)
    finally:
        spark.stop()