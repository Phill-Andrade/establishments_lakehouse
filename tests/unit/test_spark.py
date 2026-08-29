from unittest.mock import MagicMock, patch

import pytest

from template_pipeline.spark import spark_session


@patch("template_pipeline.spark.SparkSession")
def test_creates_and_stops_spark_session(
    spark_session_class: MagicMock,
) -> None:
    builder = MagicMock()
    session = MagicMock()
    spark_session_class.builder = builder
    builder.appName.return_value = builder
    builder.getOrCreate.return_value = session

    with spark_session(" template-pipeline ") as created_session:
        assert created_session is session

    builder.appName.assert_called_once_with("template-pipeline")
    builder.getOrCreate.assert_called_once_with()
    session.stop.assert_called_once_with()


@patch("template_pipeline.spark.SparkSession")
def test_stops_spark_session_after_pipeline_failure(
    spark_session_class: MagicMock,
) -> None:
    builder = MagicMock()
    session = MagicMock()
    spark_session_class.builder = builder
    builder.appName.return_value = builder
    builder.getOrCreate.return_value = session

    with pytest.raises(RuntimeError, match="pipeline failed"):
        with spark_session("template-pipeline"):
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
