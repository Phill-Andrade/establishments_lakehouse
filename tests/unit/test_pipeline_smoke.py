from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql import SparkSession

from ..integration import pipeline_smoke


def test_accepts_audit_execution_id() -> None:
    execution_id = "smoke-123-2"

    assert pipeline_smoke._require_execution_id(execution_id) == execution_id


@pytest.mark.parametrize(
    "execution_id",
    ["", "../smoke", "Smoke-123", "smoke/123", "a" * 65],
)
def test_rejects_unsafe_execution_id(execution_id: str) -> None:
    with pytest.raises(ValueError, match="Execution ID must contain"):
        pipeline_smoke._require_execution_id(execution_id)


def test_rejects_cleanup_outside_smoke_path() -> None:
    with pytest.raises(ValueError, match="Refusing to remove path"):
        pipeline_smoke._require_smoke_path("hdfs:///data/curated")


def test_rejects_execution_id_before_pipeline_side_effects() -> None:
    spark = MagicMock(spec=SparkSession)

    with pytest.raises(ValueError, match="Execution ID must contain"):
        pipeline_smoke._run_pipeline_smoke(spark, "../unsafe")

    assert spark.mock_calls == []


def test_runs_pipeline_smoke_and_cleans_output() -> None:
    spark = MagicMock(spec=SparkSession)
    output_path = "hdfs:///tmp/establishments_lakehouse/smoke/smoke-123"
    pipeline_steps = MagicMock()

    with (
        patch.object(
            pipeline_smoke,
            "_verify_hdfs_read_and_write",
        ) as verify_hdfs,
        patch.object(pipeline_smoke, "_verify_hive_access") as verify_hive,
        patch.object(pipeline_smoke, "_remove_hdfs_path") as cleanup,
    ):
        pipeline_steps.attach_mock(verify_hdfs, "verify_hdfs")
        pipeline_steps.attach_mock(verify_hive, "verify_hive")
        pipeline_steps.attach_mock(cleanup, "cleanup")
        pipeline_smoke._run_pipeline_smoke(spark, "smoke-123")

    assert pipeline_steps.mock_calls == [
        call.verify_hdfs(spark, output_path, "smoke-123"),
        call.verify_hive(spark),
        call.cleanup(spark, output_path),
    ]


def test_propagates_cleanup_failure_after_pipeline_success() -> None:
    spark = MagicMock(spec=SparkSession)
    cleanup_failure = RuntimeError("cleanup failed")
    output_path = "hdfs:///tmp/establishments_lakehouse/smoke/smoke-123"

    with (
        patch.object(pipeline_smoke, "_verify_hdfs_read_and_write"),
        patch.object(pipeline_smoke, "_verify_hive_access"),
        patch.object(
            pipeline_smoke,
            "_remove_hdfs_path",
            side_effect=cleanup_failure,
        ) as cleanup,
        pytest.raises(RuntimeError) as raised_failure,
    ):
        pipeline_smoke._run_pipeline_smoke(spark, "smoke-123")

    assert raised_failure.value is cleanup_failure
    cleanup.assert_called_once_with(spark, output_path)


def test_rejects_non_hive_catalog_before_query() -> None:
    spark = MagicMock(spec=SparkSession)
    spark.conf.get.return_value = "in-memory"

    with pytest.raises(RuntimeError, match="Hive catalog is required"):
        pipeline_smoke._verify_hive_access(spark)

    spark.sql.assert_not_called()


def test_queries_hive_catalog() -> None:
    spark = MagicMock(spec=SparkSession)
    spark.conf.get.return_value = "hive"

    pipeline_smoke._verify_hive_access(spark)

    spark.conf.get.assert_called_once_with("spark.sql.catalogImplementation")
    spark.sql.assert_called_once_with("SHOW DATABASES")
    spark.sql.return_value.take.assert_called_once_with(1)


def test_cleans_hdfs_after_pipeline_failure() -> None:
    spark = MagicMock(spec=SparkSession)
    output_path = "hdfs:///tmp/establishments_lakehouse/smoke/smoke-123"

    with (
        patch.object(
            pipeline_smoke,
            "_verify_hdfs_read_and_write",
            side_effect=RuntimeError("pipeline failed"),
        ),
        patch.object(pipeline_smoke, "_remove_hdfs_path") as cleanup,
        pytest.raises(RuntimeError, match="pipeline failed"),
    ):
        pipeline_smoke._run_pipeline_smoke(spark, "smoke-123")

    cleanup.assert_called_once_with(spark, output_path)


def test_preserves_pipeline_failure_when_cleanup_also_fails() -> None:
    spark = MagicMock(spec=SparkSession)
    pipeline_failure = RuntimeError("pipeline failed")

    with (
        patch.object(
            pipeline_smoke,
            "_verify_hdfs_read_and_write",
            side_effect=pipeline_failure,
        ),
        patch.object(
            pipeline_smoke,
            "_remove_hdfs_path",
            side_effect=RuntimeError("cleanup failed"),
        ),
        pytest.raises(RuntimeError) as raised_failure,
    ):
        pipeline_smoke._run_pipeline_smoke(spark, "smoke-123")

    assert raised_failure.value is pipeline_failure
