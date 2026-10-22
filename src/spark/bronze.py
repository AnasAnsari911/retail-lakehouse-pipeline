"""Bronze layer: load the raw CRM and ERP CSV files as-is into the lake.

Bronze rules:
- no cleaning or transformation of the source data
- add two technical columns (dwh_load_date, dwh_source_file)
- full load: every run overwrites the previous output (truncate and insert)
"""

import logging

from pyspark.sql import functions as F

from src.common import config
from src.common.schemas import (
    CUST_AZ12_SCHEMA,
    CUST_INFO_SCHEMA,
    LOC_A101_SCHEMA,
    PRD_INFO_SCHEMA,
    PX_CAT_G1V2_SCHEMA,
    SALES_DETAILS_SCHEMA,
)
from src.common.spark_session import get_spark


logger = logging.getLogger(__name__)


def load_table(spark, source, file_name, table_name, schema):
    """Read one CSV, add tracking columns, write it to bronze as Parquet.

    Returns the number of rows that were saved.
    """
    # Full path: <project root>/datasets/<source>/<file_name>
    file_path = config.DATASETS / source / file_name

    # Read the CSV with the explicit schema (no guessing of column types)
    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(str(file_path))
    )

    # Technical metadata columns (dwh_ prefix, per our naming conventions)
    df = (
        df
        .withColumn("dwh_load_date", F.current_timestamp())   # when it was loaded
        .withColumn("dwh_source_file", F.input_file_name())   # which file it came from
    )

    # mode("overwrite") replaces old data, so reruns never create duplicates
    output_path = config.BRONZE_DIR / table_name
    df.write.mode("overwrite").parquet(str(output_path))

    # Count what was actually saved, not the CSV in memory
    return spark.read.parquet(str(output_path)).count()


# (source folder, file name, bronze table name, schema)
TABLES = [
    ("crm", "cust_info.csv", "crm_cust_info", CUST_INFO_SCHEMA),
    ("crm", "prd_info.csv", "crm_prd_info", PRD_INFO_SCHEMA),
    ("crm", "sales_details.csv", "crm_sales_details", SALES_DETAILS_SCHEMA),
    ("erp", "CUST_AZ12.csv", "erp_cust_az12", CUST_AZ12_SCHEMA),
    ("erp", "LOC_A101.csv", "erp_loc_a101", LOC_A101_SCHEMA),
    ("erp", "PX_CAT_G1V2.csv", "erp_px_cat_g1v2", PX_CAT_G1V2_SCHEMA),
]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    spark = get_spark("load_bronze")

    try :
        for source, file_name, table_name, schema in TABLES:
            try :
                count = load_table(spark, source, file_name, table_name, schema)
            except Exception as e:
                logger.error(f"Failed to load {table_name}: {e}") 
                raise  
            logger.info(f"{table_name} : {count}")
    finally:
        spark.stop()