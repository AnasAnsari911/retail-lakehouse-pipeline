from src.common import config
from src.common.spark_session import get_spark

print("Project root:", config.ROOT)

spark = get_spark()
spark.createDataFrame([(1, "Ali"), (2, "Sara")], ["id", "name"]).show()
spark.stop()