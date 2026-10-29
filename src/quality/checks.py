"""
Data quality checks for the pipeline.

Each check returns a result (a dictionary) instead of crashing, so every
check runs and every result can be saved to the audit table.
"""

from src.common import config
from src.common.spark_session import get_spark

from src.common.postgres import jdbc_options
from pyspark.sql import functions as F


def make_result(layer, table_name, check_name, passed, expected, actual, details=""):
    """Build one result row in the shape of audit.dq_results."""
    return {
        "layer": layer,
        "table_name": table_name,
        "check_name": check_name,
        "status": "PASS" if passed else "FAIL",
        "expected": str(expected),
        "actual": str(actual),
        "details": details,
    }


def check_not_empty(df, layer, table_name):
    """Pass if the table has at least one row."""
    row_count = df.count()
    return make_result(
        layer, table_name, "row_count_not_empty",
        passed=row_count > 0,
        expected="> 0",
        actual=row_count,
    )


def check_unique(df, column, layer, table_name):
    """Pass if every value in the column appears only once."""
    total_rows = df.count()
    distinct_values = df.select(column).distinct().count()
    return make_result(
        layer, table_name, f"{column}_is_unique",
        passed=total_rows == distinct_values,
        expected= total_rows,
        actual= distinct_values,
    )


def check_no_nulls(df, column, layer, table_name):
    """Pass if the column has no null (empty) values."""
    null_count = df.filter(F.col(column).isNull()).count()
    return make_result(
        layer, table_name, f"{column}_has_no_nulls",
        passed= null_count == 0,
        expected= 0,
        actual= null_count,
    )

def check_no_bad_rows(df, bad_condition, layer, table_name, check_name):
    """Pass if no row matches bad_condition (a rule that describes a bad row)."""
    bad_count = df.filter(bad_condition).count()
    return make_result(
        layer, table_name, check_name,
        passed=bad_count == 0,
        expected=0,
        actual=bad_count,
    )


def check_equal(expected, actual, layer, table_name, check_name, details=""):
    """Pass if the two values are equal. Used to compare numbers between layers."""
    return make_result(
        layer, table_name, check_name,
        passed= expected == actual,
        expected= expected,
        actual= actual,
        details= details,
    )


def read_postgres(spark, table_name):
    """Read one gold table from PostgreSQL into a DataFrame."""   
    return (
        spark.read
        .format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", f"gold.{table_name}")
        .load()
    )

def run_all_checks(spark):
    """Run every data quality check and return the list of results."""
    results = []

    # Load the tables once, reuse them for several checks
    customers = spark.read.parquet(str(config.GOLD_DIR / "dim_customers"))
    products = spark.read.parquet(str(config.GOLD_DIR / "dim_products"))
    sales = spark.read.parquet(str(config.GOLD_DIR / "fact_sales"))
    silver_sales = spark.read.parquet(str(config.SILVER_DIR / "crm_sales_details"))

    # --- Checks on one table at a time ---
    results.append(check_not_empty(customers, "gold", "dim_customers"))
    results.append(check_unique(customers, "customer_key", "gold", "dim_customers"))
    results.append(check_no_nulls(customers, "customer_key", "gold", "dim_customers"))

    results.append(check_not_empty(products, "gold", "dim_products"))
    results.append(check_unique(products, "product_key", "gold", "dim_products"))
    results.append(check_no_nulls(products, "product_key", "gold", "dim_products"))

    results.append(check_not_empty(sales, "gold", "fact_sales"))
    results.append(check_no_bad_rows(
        sales,
        F.col("customer_key").isNull() | F.col("product_key").isNull(),
        "gold", "fact_sales", "no_orphan_sales",
    ))
    results.append(check_no_bad_rows(
        sales,
        F.col("sales_amount") != F.col("quantity") * F.col("price"),
        "gold", "fact_sales", "sales_equals_quantity_times_price",
    ))

    # --- Comparisons between layers ---
    results.append(check_equal(
        silver_sales.count(), sales.count(),
        "reconciliation", "fact_sales", "silver_to_gold_row_count",
        details="crm_sales_details vs fact_sales",
    ))

    for table_name, lake_df in [
        ("dim_customers", customers),
        ("dim_products", products),
        ("fact_sales", sales),
    ]:
        results.append(check_equal(
            lake_df.count(), read_postgres(spark, table_name).count(),
            "reconciliation", table_name, "lake_to_postgres_row_count",
            details="lake/gold vs PostgreSQL",
        ))

    lake_total = sales.agg(F.sum("sales_amount")).collect()[0][0]
    pg_total = read_postgres(spark, "fact_sales").agg(F.sum("sales_amount")).collect()[0][0]
    results.append(check_equal(
        lake_total, pg_total,
        "reconciliation", "fact_sales", "lake_to_postgres_sales_total",
        details="SUM(sales_amount)",
    ))

    return results

