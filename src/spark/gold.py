"""
Gold layer: build the star schema from the silver tables.

Reads  : lake/silver/<table>   (cleaned Parquet from silver.py)
Writes : lake/gold/<table>     (dim_customers, dim_products, fact_sales)

Business rules (ported from the SQL Server gold views):
  - dim_customers : CRM customers + ERP birthdate/country, CRM is the primary
                    source for gender, ERP is the fallback.
  - dim_products  : CRM products + ERP categories, current products only
                    (prd_end_dt is null).
  - fact_sales    : sales joined to both dimensions to pick up surrogate keys.

Every run overwrites the gold tables, so the job is safe to re-run.

Run from the project root:  python -m src.spark.gold
"""
import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common import config
from src.common.spark_session import get_spark

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def read_silver(spark, table_name):
    """Read one silver table from lake/silver/<table_name>."""
    return spark.read.parquet(str(config.SILVER_DIR / table_name))


def read_gold(spark, table_name):
    """Read one gold table from lake/gold/<table_name>."""
    return spark.read.parquet(str(config.GOLD_DIR / table_name))


def write_gold(spark, df, table_name):
    """Overwrite lake/gold/<table_name> and return the row count read back."""
    df.write.mode("overwrite").parquet(str(config.GOLD_DIR / table_name))
    return read_gold(spark, table_name).count()


# ---------------------------------------------------------------------------
# Build functions (one per gold table)
# ---------------------------------------------------------------------------
def build_dim_customers(spark):
    crm_cust_info = read_silver(spark, "crm_cust_info")
    erp_cust_az12 = read_silver(spark, "erp_cust_az12")
    erp_loc_a101 = read_silver(spark, "erp_loc_a101")

    df = (
        crm_cust_info.alias("ci")
        .join(erp_cust_az12.alias("ca"), F.col("ci.cst_key") == F.col("ca.cid"), how="left")
        .join(erp_loc_a101.alias("la"), F.col("ci.cst_key") == F.col("la.cid"), how="left")
        .select(
            # Surrogate key: 1, 2, 3... ordered by the source customer id
            F.row_number().over(Window.orderBy(F.col("ci.cst_id"))).alias("customer_key"),
            F.col("ci.cst_id").alias("customer_id"),
            F.col("ci.cst_key").alias("customer_number"),
            F.col("ci.cst_firstname").alias("first_name"),
            F.col("ci.cst_lastname").alias("last_name"),
            F.col("la.cntry").alias("country"),
            F.col("ci.cst_marital_status").alias("marital_status"),
            # CRM gender first; if it is 'n/a' fall back to ERP, then to 'n/a'
            F.coalesce(
                F.when(F.col("ci.cst_gndr") != "n/a", F.col("ci.cst_gndr")),
                F.col("ca.gen"),
                F.lit("n/a"),
            ).alias("gender"),
            F.col("ca.bdate").alias("birthdate"),
            F.col("ci.cst_create_date").alias("create_date"),
        )
    )
    return df


def build_dim_products(spark):
    crm_prd_info = read_silver(spark, "crm_prd_info")
    erp_px_cat_g1v2 = read_silver(spark, "erp_px_cat_g1v2")

    df = (
        crm_prd_info.alias("pn")
        .join(erp_px_cat_g1v2.alias("pc"), F.col("pn.cat_id") == F.col("pc.id"), how="left")
        # Keep only current products, drop historical price versions
        .filter(F.col("pn.prd_end_dt").isNull())
        .select(
            # Surrogate key ordered by start date, then product key
            F.row_number().over(
                Window.orderBy(F.col("pn.prd_start_dt"), F.col("pn.prd_key"))
            ).alias("product_key"),
            F.col("pn.prd_id").alias("product_id"),
            F.col("pn.prd_key").alias("product_number"),
            F.col("pn.prd_nm").alias("product_name"),
            F.col("pn.cat_id").alias("category_id"),
            F.col("pc.cat").alias("category"),
            F.col("pc.subcat").alias("subcategory"),
            F.col("pc.maintenance").alias("maintenance"),
            F.col("pn.prd_cost").alias("cost"),
            F.col("pn.prd_line").alias("product_line"),
            F.col("pn.prd_start_dt").alias("start_date"),
        )
    )
    return df


def build_fact_sales(spark, dim_customers, dim_products):
    crm_sales_details = read_silver(spark, "crm_sales_details")

    df = (
        crm_sales_details.alias("sd")
        .join(dim_products.alias("pr"), F.col("sd.sls_prd_key") == F.col("pr.product_number"), how="left")
        .join(dim_customers.alias("cu"), F.col("sd.sls_cust_id") == F.col("cu.customer_id"), how="left")
        .select(
            F.col("sd.sls_ord_num").alias("order_number"),
            F.col("pr.product_key").alias("product_key"),     # surrogate keys,
            F.col("cu.customer_key").alias("customer_key"),   # not source ids
            F.col("sd.sls_order_dt").alias("order_date"),
            F.col("sd.sls_ship_dt").alias("shipping_date"),
            F.col("sd.sls_due_dt").alias("due_date"),
            F.col("sd.sls_sales").alias("sales_amount"),
            F.col("sd.sls_quantity").alias("quantity"),
            F.col("sd.sls_price").alias("price"),
        )
    )
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    spark = get_spark("load_gold")

    try:
        # 1. Dimensions first: the fact table needs their surrogate keys
        count = write_gold(spark, build_dim_customers(spark), "dim_customers")
        logger.info("dim_customers: %s rows", count)

        count = write_gold(spark, build_dim_products(spark), "dim_products")
        logger.info("dim_products: %s rows", count)

        # 2. Read the saved dimensions back so the fact joins against exactly
        #    the keys that were written to gold
        dim_customers = read_gold(spark, "dim_customers")
        dim_products = read_gold(spark, "dim_products")

        # 3. Fact table
        count = write_gold(spark, build_fact_sales(spark, dim_customers, dim_products), "fact_sales")
        logger.info("fact_sales: %s rows", count)

    except Exception as e:
        logger.error("Gold load failed: %s", e)
        raise
    finally:
        spark.stop()