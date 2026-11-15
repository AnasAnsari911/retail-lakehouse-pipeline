from src.common.spark_session import get_spark

spark = get_spark("init_lakehouse")

for layer in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{layer}")

spark.sql("SHOW NAMESPACES IN lakehouse").show()
spark.stop()