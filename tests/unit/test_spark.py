from unittest.mock import MagicMock, patch

import pytest

from establishments_lakehouse.spark import spark_session


@patch("establishments_lakehouse.spark.SparkSession")
def test_creates_and_stops_spark_session(
    spark_session_class: MagicMock,
) -> None:
    builder = MagicMock()
    session = MagicMock()
    spark_session_class.builder = builder
    builder.appName.return_value = builder
    builder.getOrCreate.return_value = session

    with spark_session(" establishments-lakehouse ") as created_session:
        assert created_session is session

    builder.appName.assert_called_once_with("establishments-lakehouse")
    builder.getOrCreate.assert_called_once_with()
    session.stop.assert_called_once_with()


@patch("establishments_lakehouse.spark.SparkSession")
def test_stops_spark_session_after_pipeline_failure(
    spark_session_class: MagicMock,
) -> None:
    builder = MagicMock()
    session = MagicMock()
    spark_session_class.builder = builder
    builder.appName.return_value = builder
    builder.getOrCreate.return_value = session

    with pytest.raises(RuntimeError, match="pipeline failed"):
        with spark_session("establishments-lakehouse"):
            raise RuntimeError("pipeline failed")

    session.stop.assert_called_once_with()


def test_rejects_non_string_application_name() -> None:
    with pytest.raises(
        TypeError,
        match="Application name must be a string",
    ):
        with spark_session(None):  # type: ignore[arg-type]
            pass


def test_rejects_empty_application_name() -> None:
    with pytest.raises(
        ValueError,
        match="Application name cannot be empty",
    ):
        with spark_session(" "):
            pass
