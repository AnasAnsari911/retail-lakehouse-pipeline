from src.common.spark_session import get_spark

spark = get_spark("iceberg_smoke")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke")
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.smoke.hello (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO lakehouse.smoke.hello VALUES (1, 'iceberg works')")

spark.sql("SELECT * FROM lakehouse.smoke.hello").show()
spark.sql("SELECT snapshot_id, operation FROM lakehouse.smoke.hello.snapshots").show()

spark.stop()