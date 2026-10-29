"""
Run all data quality checks, save the results to audit.dq_results in
PostgreSQL, and exit with an error if any check failed.

Run from the project root:  python -m src.quality.run_checks
"""

import logging
import sys
import uuid

from pyspark.sql.types import StringType, StructField, StructType

from src.common.postgres import jdbc_options
from src.common.spark_session import get_spark
from src.quality.checks import run_all_checks

logger = logging.getLogger(__name__)

# Columns we send. id and run_ts are left out: PostgreSQL fills them in itself.
RESULT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("layer", StringType()),
    StructField("table_name", StringType()),
    StructField("check_name", StringType()),
    StructField("status", StringType()),
    StructField("expected", StringType()),
    StructField("actual", StringType()),
    StructField("details", StringType()),
])


def save_results(spark, results, run_id):
    """Append the check results to audit.dq_results."""
    rows = [
        (
            run_id,
            r["layer"],
            r["table_name"],
            r["check_name"],
            r["status"],
            r["expected"],
            r["actual"],
            r["details"],
        )
        for r in results
    ]
    df = spark.createDataFrame(rows, RESULT_SCHEMA)

    (
        df.write
        .format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", "audit.dq_results")
        .mode("append")   # add rows, keep the history of earlier runs
        .save()
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    spark = get_spark("run_checks")
    exit_code = 0

    try:
        run_id = str(uuid.uuid4())   # one random ID shared by all checks in this run
        results = run_all_checks(spark)
        save_results(spark, results, run_id)

        failed = [r for r in results if r["status"] == "FAIL"]
        logger.info(f"Run {run_id}: {len(results)} checks, {len(failed)} failed.")

        for r in failed:
            logger.error(
                f"FAILED {r['table_name']} / {r['check_name']}: "
                f"expected {r['expected']}, actual {r['actual']}"
            )

        if failed:
            exit_code = 1

    except Exception as e:
        logger.error(f"Quality run crashed: {e}")
        raise
    finally:
        spark.stop()

    sys.exit(exit_code)