"""
Publish the gold layer from the data lake to PostgreSQL.

Reads  : lake/gold/<table>   (Parquet written by gold.py)
Writes : gold.<table>        (PostgreSQL tables created by sql/postgres/01_create_gold.sql)

Tables are loaded dimensions first, then the fact table, because the fact
table has foreign keys pointing at both dimensions.

Every run empties the tables and reloads them (TRUNCATE + INSERT), so the job
is safe to re-run and the table definitions (types, keys) stay intact.

Credentials come from the .env file, never from the code.

Run from the project root:  python -m src.spark.publish_gold
"""
import logging

from src.common import config
from src.common.spark_session import get_spark
from src.common.postgres import jdbc_options

logger = logging.getLogger(__name__)

# Order matters: dimensions before the fact table (foreign keys)
TABLES = [
    "dim_customers",
    "dim_products",
    "fact_sales",
]


def publish_table(spark, table_name):
    """Load lake/gold/<table_name> into Postgres gold.<table_name>
    and return the row count read back from Postgres."""
    df = spark.read.parquet(str(config.GOLD_DIR / table_name))
    db_table = f"gold.{table_name}"

    (
        df.write
        .format("jdbc")
        .option("dbtable", db_table)
        .options(**jdbc_options())
        # truncate: empty the existing table instead of dropping it, so the
        # primary keys, foreign keys and column types from the DDL are kept.
        .option("truncate", True)
        # cascadeTruncate: Postgres refuses to truncate a table that other
        # tables reference (dims -> fact) unless CASCADE is used.
        .option("cascadeTruncate", True)
        .mode("overwrite")
        .save()
    )

    # Read the table back from Postgres to prove the data really arrived
    result_df = (
        spark.read
        .format("jdbc")
        .option("dbtable", db_table)
        .options(**jdbc_options())
        .load()
    )

    return result_df.count()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    spark = get_spark("publish_gold")

    try:
        for table_name in TABLES:
            count = publish_table(spark, table_name)
            logger.info(f"Row count for {table_name} : {count}")

    except Exception as e:
        logger.error(f"Failed to publish table: {e}")
        raise
    finally:
        spark.stop()