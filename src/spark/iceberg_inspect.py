from src.common.spark_session import get_spark

spark = get_spark("iceberg_inspect")

print("=== Snapshots of gold.fact_sales ===")

spark.sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold.fact_sales.snapshots
    ORDER BY committed_at
""").show(truncate=False)

print("=== Data files behind the current version ===")

spark.sql("""
    SELECT file_path, record_count,  file_size_in_bytes
    FROM lakehouse.gold.fact_sales.files
""").show(truncate=False)

spark.stop()