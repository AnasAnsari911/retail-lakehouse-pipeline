"""Demo: Iceberg snapshots and rollback on a throwaway table.

Shows that a bad change (a wrong insert and an accidental delete) can be
undone by moving the table back to an earlier snapshot. Safe to re-run: the
demo table is rebuilt at the start and dropped at the end.

Run from the project root:  python -m src.spark.iceberg_rollback_demo
"""

from src.common.spark_session import get_spark

TABLE = "lakehouse.demo.rollback_demo"


def latest_snapshot(spark):
    """Return the id of the newest snapshot of the demo table."""
    return spark.sql(
        f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0][0]


def show(spark, title):
    """Print the demo table's rows under a title."""
    print(f"=== {title} ===")
    spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").show(truncate=False)


if __name__ == "__main__":
    spark = get_spark("iceberg_rollback_demo")

    try:
        # Start from a clean table so every run gives the same result
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.demo")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
        spark.sql(f"CREATE TABLE {TABLE} (id INT, name STRING) USING iceberg")

        # 1. A good state, and remember its snapshot id
        spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'good row'), (2, 'good row')")
        good_snapshot = latest_snapshot(spark)
        show(spark, "Good state")

        # 2. Two mistakes
        spark.sql(f"INSERT INTO {TABLE} VALUES (3, 'BAD row')")
        spark.sql(f"DELETE FROM {TABLE} WHERE id = 1")
        show(spark, "After the mistakes")

        # 3. Undo: move the table back to the good snapshot (no data is copied)
        spark.sql(f"CALL lakehouse.system.rollback_to_snapshot('demo.rollback_demo', {good_snapshot})")
        show(spark, "After rollback")

        # Clean up the throwaway table
        spark.sql(f"DROP TABLE {TABLE} PURGE")
    finally:
        spark.stop()