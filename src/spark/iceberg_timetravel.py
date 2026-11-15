from src.common.spark_session import get_spark

spark = get_spark("iceberg_timetravel")

OLD_SNAPSHOT = 2090440514241178154


print("=== fact sales as it was at the OLD snapshot ===")

spark.sql(f"""
    SELECT COUNT(*) AS row_count, SUM(sales_amount) AS total_sales
    FROM lakehouse.gold.fact_sales VERSION AS OF {OLD_SNAPSHOT}
""").show(truncate=False)

print("===  fact_sales NOW ===")
spark.sql("""
    SELECT COUNT(*) AS row_count, SUM(sales_amount) AS total_sales
    FROM lakehouse.gold.fact_sales
""").show()

spark.stop()