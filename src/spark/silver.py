"""Silver layer: clean and standardise the bronze tables.

Silver rules:
- read each table from the bronze layer (lake/bronze)
- apply the cleaning rules ported from the SQL Server load_silver procedure
- keep only the business columns and add a fresh technical column (dwh_load_date)
- full load: every run overwrites the previous output (truncate and insert)

Each table has its own clean_* function that takes a DataFrame and returns
a cleaned DataFrame. load_silver_table() reads, cleans and saves one table.
"""

import logging

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src.common import config
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CRM tables
# ---------------------------------------------------------------------------

def clean_crm_cust_info(df):
    """Remove junk and duplicate customers, trim names, spell out codes."""

    # Rows without a customer id are junk, so drop them
    df = df.filter(F.col("cst_id").isNotNull())

    # Keep only the newest record for each customer (latest create date)
    window_spec = (
        Window
        .partitionBy("cst_id")
        .orderBy(F.col("cst_create_date").desc())
    )
    df = (
        df
        .withColumn("row_number", F.row_number().over(window_spec))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    # Remove extra spaces from names
    df = (
        df
        .withColumn("cst_firstname", F.trim(F.col("cst_firstname")))
        .withColumn("cst_lastname", F.trim(F.col("cst_lastname")))
    )

    # Turn codes into readable words (anything unknown becomes 'n/a')
    df = df.withColumn(
        "cst_marital_status",
        F.when(F.upper(F.trim(F.col("cst_marital_status"))) == "S", "Single")
        .when(F.upper(F.trim(F.col("cst_marital_status"))) == "M", "Married")
        .otherwise("n/a")
    )
    df = df.withColumn(
        "cst_gndr",
        F.when(F.upper(F.trim(F.col("cst_gndr"))) == "M", "Male")
        .when(F.upper(F.trim(F.col("cst_gndr"))) == "F", "Female")
        .otherwise("n/a")
    )

    # Keep only the business columns (drops bronze's technical columns)
    df = df.select(
        "cst_id",
        "cst_key",
        "cst_firstname",
        "cst_lastname",
        "cst_marital_status",
        "cst_gndr",
        "cst_create_date",
    )

    # Fresh load timestamp for silver
    return df.withColumn("dwh_load_date", F.current_timestamp())


def clean_crm_prd_info(df):
    """Split the product key, fill missing costs, name product lines, build end dates."""

    # First 5 characters of the key are the category id (CO-RF becomes CO_RF)
    df = df.withColumn(
        "cat_id",
        F.regexp_replace(F.substring(F.col("prd_key"), 1, 5), "-", "_")
    )

    # Dates arrive as text in bronze, so cast them to real dates
    df = df.withColumn("prd_start_dt", F.col("prd_start_dt").cast(DateType()))

    # End date = day before the next start date of the SAME product.
    # This uses the original full prd_key, so it must run before prd_key is cut.
    window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    df = df.withColumn(
        "prd_end_dt",
        F.date_sub(F.lead("prd_start_dt", 1).over(window_spec), 1)
    )

    # Product key = everything after the category prefix (from character 7 on)
    df = df.withColumn("prd_key", F.substring("prd_key", 7, 100))

    # Missing cost becomes 0
    df = df.fillna(0, subset=["prd_cost"])

    # Turn product line codes into readable words
    df = df.withColumn("prd_line", F.upper(F.trim(F.col("prd_line"))))
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
        "prd_end_dt",
    )

    return df.withColumn("dwh_load_date", F.current_timestamp())


def _parse_yyyymmdd(col_name):
    """Turn an integer like 20101229 into a date. 0 or a wrong length becomes null."""
    c = F.col(col_name)
    return F.when(
        c.isNull() | (c == 0) | (F.length(c.cast("string")) != 8),
        F.lit(None),
    ).otherwise(F.to_date(c.cast("string"), "yyyyMMdd"))


def clean_crm_sales_details(df):
    """Convert integer dates to real dates and repair wrong sales and price values."""

    # These two rules are applied together in ONE select below, so both read the
    # ORIGINAL sales and price columns (the same way the SQL SELECT does).

    # Sales: if missing, not positive, or not equal to quantity * price,
    # recalculate it as quantity * price
    new_sales = F.when(
        F.col("sls_sales").isNull()
        | (F.col("sls_sales") <= 0)
        | (F.col("sls_sales") != F.col("sls_quantity") * F.abs(F.col("sls_price"))),
        F.col("sls_quantity") * F.abs(F.col("sls_price")),
    ).otherwise(F.col("sls_sales"))

    # Price: if missing or not positive, derive it as sales / quantity
    # (nullif avoids dividing by zero; cast to int to match SQL Server's integer maths)
    new_price = (
        F.when(
            F.col("sls_price").isNull() | (F.col("sls_price") <= 0),
            F.col("sls_sales") / F.expr("nullif(sls_quantity, 0)"),
        )
        .otherwise(F.col("sls_price"))
        .cast("int")
    )

    df = df.select(
        "sls_ord_num",
        "sls_prd_key",
        "sls_cust_id",
        _parse_yyyymmdd("sls_order_dt").alias("sls_order_dt"),
        _parse_yyyymmdd("sls_ship_dt").alias("sls_ship_dt"),
        _parse_yyyymmdd("sls_due_dt").alias("sls_due_dt"),
        new_sales.alias("sls_sales"),
        "sls_quantity",
        new_price.alias("sls_price"),
    )

    return df.withColumn("dwh_load_date", F.current_timestamp())


# ---------------------------------------------------------------------------
# ERP tables
# ---------------------------------------------------------------------------

def clean_erp_cust_az12(df):
    """Remove the NAS prefix from ids, null out future birthdates, standardise gender."""

    # Some ids start with 'NAS'; remove those 3 characters so ids match the CRM
    df = df.withColumn(
        "cid",
        F.when(F.col("cid").startswith("NAS"), F.substring("cid", 4, 100))
        .otherwise(F.col("cid"))
    )

    # A birthdate in the future is invalid, so set it to null
    df = df.withColumn(
        "bdate",
        F.when(F.col("bdate") > F.current_date(), F.lit(None))
        .otherwise(F.col("bdate"))
    )

    # Standardise gender values (anything unknown becomes 'n/a')
    df = df.withColumn(
        "gen",
        F.when(F.trim(F.upper(F.col("gen"))).isin("F", "FEMALE"), "Female")
        .when(F.trim(F.upper(F.col("gen"))).isin("M", "MALE"), "Male")
        .otherwise("n/a")
    )

    df = df.select("cid", "bdate", "gen")

    return df.withColumn("dwh_load_date", F.current_timestamp())


def clean_erp_loc_a101(df):
    """Remove dashes from ids and standardise country names."""

    # AW-00011000 becomes AW00011000 so it matches the CRM customer key
    df = df.withColumn("cid", F.regexp_replace(F.col("cid"), "-", ""))

    # Country codes become full names; blank or missing becomes 'n/a'
    df = df.withColumn(
        "cntry",
        F.when(F.trim(F.upper(F.col("cntry"))).isin("US", "USA"), "United States")
        .when(F.trim(F.upper(F.col("cntry"))) == "DE", "Germany")
        .when((F.trim(F.col("cntry")) == "") | F.col("cntry").isNull(), "n/a")
        .otherwise(F.trim(F.col("cntry")))
    )

    df = df.select("cid", "cntry")

    return df.withColumn("dwh_load_date", F.current_timestamp())


def clean_erp_px_cat_g1v2(df):
    """No cleaning needed: copy the business columns and add the load date."""

    df = df.select("id", "cat", "subcat", "maintenance")

    return df.withColumn("dwh_load_date", F.current_timestamp())


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

# (table name, cleaning function). Names match the bronze tables.
SILVER_TABLES = [
    ("crm_cust_info", clean_crm_cust_info),
    ("crm_prd_info", clean_crm_prd_info),
    ("crm_sales_details", clean_crm_sales_details),
    ("erp_cust_az12", clean_erp_cust_az12),
    ("erp_loc_a101", clean_erp_loc_a101),
    ("erp_px_cat_g1v2", clean_erp_px_cat_g1v2),
]


def load_silver_table(spark, table_name, clean_func):
    """Read a bronze table, clean it, save it to silver. Returns the saved row count."""
    df = spark.read.parquet(str(config.BRONZE_DIR / table_name))
    df = clean_func(df)

    # mode("overwrite") replaces old data, so reruns never create duplicates
    df.write.mode("overwrite").parquet(str(config.SILVER_DIR / table_name))

    # Count what was actually saved
    return spark.read.parquet(str(config.SILVER_DIR / table_name)).count()


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    spark = get_spark("silver_load")

    try:
        for table_name, clean_func in SILVER_TABLES:
            try:
                count = load_silver_table(spark, table_name, clean_func)
            except Exception as e:
                logger.error(f"Failed to load {table_name}: {e}")
                raise  # stop the run, because later steps need these tables
            logger.info(f"{table_name} : {count}")
    finally:
        spark.stop()