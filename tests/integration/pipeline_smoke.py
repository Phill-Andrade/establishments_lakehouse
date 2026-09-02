import argparse
import logging
import re

from pyspark.sql import SparkSession

from establishments_lakehouse.spark import spark_session


_APPLICATION_NAME = "establishments-lakehouse-pipeline-smoke"
_HDFS_BASE_PATH = "hdfs:///tmp/establishments_lakehouse/smoke"
_HDFS_WRITE_MODE = "errorifexists"
_HIVE_CATALOG_IMPLEMENTATION = "hive"
_HIVE_CATALOG_SETTING = "spark.sql.catalogImplementation"
_HIVE_DATABASES_QUERY = "SHOW DATABASES"
_SMOKE_STATUS = "ok"
_RECORD_VALIDATION_LIMIT = 2
_EXECUTION_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_RECURSIVE_DELETE = True
_LOGGER = logging.getLogger(__name__)


def main() -> None:
    execution_id = _parse_execution_id()
    with spark_session(_APPLICATION_NAME) as spark:
        _run_pipeline_smoke(spark, execution_id)

    print("Pipeline smoke test succeeded.")


def _parse_execution_id() -> str:
    parser = argparse.ArgumentParser(
        description="Validate the pipeline integration with the data platform."
    )
    parser.add_argument("--execution-id", required=True)
    arguments = parser.parse_args()
    return _require_execution_id(arguments.execution_id)


def _require_execution_id(execution_id: str) -> str:
    if not _EXECUTION_ID_PATTERN.fullmatch(execution_id):
        raise ValueError(
            "Execution ID must contain only lowercase letters, numbers, "
            "underscores, or hyphens and have at most 64 characters."
        )

    return execution_id


def _smoke_output_path(execution_id: str) -> str:
    validated_execution_id = _require_execution_id(execution_id)
    return f"{_HDFS_BASE_PATH}/{validated_execution_id}"


def _run_pipeline_smoke(
    spark: SparkSession,
    execution_id: str,
) -> None:
    output_path = _smoke_output_path(execution_id)

    try:
        _verify_hdfs_read_and_write(spark, output_path, execution_id)
        _verify_hive_access(spark)
    except Exception:
        _clean_hdfs_after_failure(spark, output_path)
        raise

    _remove_hdfs_path(spark, output_path)


def _clean_hdfs_after_failure(
    spark: SparkSession,
    output_path: str,
) -> None:
    try:
        _remove_hdfs_path(spark, output_path)
    except Exception:
        _LOGGER.exception(
            "Could not clean HDFS path %s after pipeline smoke failure.",
            output_path,
        )


def _verify_hdfs_read_and_write(
    spark: SparkSession,
    output_path: str,
    execution_id: str,
) -> None:
    source = spark.createDataFrame(
        [(execution_id, _SMOKE_STATUS)],
        ["execution_id", "status"],
    )
    source.write.mode(_HDFS_WRITE_MODE).parquet(output_path)

    expected = {
        "execution_id": execution_id,
        "status": _SMOKE_STATUS,
    }
    records = spark.read.parquet(output_path).take(_RECORD_VALIDATION_LIMIT)
    actual = [record.asDict() for record in records]

    if actual != [expected]:
        raise RuntimeError(
            f"HDFS smoke data mismatch: expected {[expected]}, "
            f"received {actual}."
        )


def _verify_hive_access(spark: SparkSession) -> None:
    catalog_implementation = spark.conf.get(_HIVE_CATALOG_SETTING)
    if catalog_implementation != _HIVE_CATALOG_IMPLEMENTATION:
        raise RuntimeError(
            "Hive catalog is required: expected "
            f"{_HIVE_CATALOG_IMPLEMENTATION!r}, "
            f"received {catalog_implementation!r}."
        )

    spark.sql(_HIVE_DATABASES_QUERY).take(1)


def _remove_hdfs_path(spark: SparkSession, output_path: str) -> None:
    _require_smoke_path(output_path)

    spark_context = spark.sparkContext
    hadoop = spark_context._jvm.org.apache.hadoop
    configuration = spark_context._jsc.hadoopConfiguration()
    hadoop_path = hadoop.fs.Path(output_path)
    filesystem = hadoop_path.getFileSystem(configuration)

    if not filesystem.exists(hadoop_path):
        return

    if not filesystem.delete(hadoop_path, _RECURSIVE_DELETE):
        raise RuntimeError(f"Could not delete HDFS smoke path: {output_path}")


def _require_smoke_path(output_path: str) -> None:
    expected_prefix = f"{_HDFS_BASE_PATH}/"
    if not output_path.startswith(expected_prefix):
        raise ValueError(f"Refusing to remove path outside {_HDFS_BASE_PATH}.")

    execution_id = output_path.removeprefix(expected_prefix)
    _require_execution_id(execution_id)


if __name__ == "__main__":
    main()
