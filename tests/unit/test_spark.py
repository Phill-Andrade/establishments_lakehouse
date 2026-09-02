from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from establishments_lakehouse.spark import spark_session


@pytest.fixture
def spark_session_mock() -> MagicMock:
    return MagicMock(spec=SparkSession)


@pytest.fixture
def spark_builder_mock(
    spark_session_mock: MagicMock,
) -> Generator[MagicMock, None, None]:
    builder = MagicMock(spec=SparkSession.Builder)

    with patch("establishments_lakehouse.spark.SparkSession") as session_class:
        session_class.builder = builder
        builder.appName.return_value = builder
        builder.getOrCreate.return_value = spark_session_mock
        yield builder


def test_normalizes_application_name(
    spark_builder_mock: MagicMock,
) -> None:
    with spark_session(" establishments-lakehouse "):
        pass

    spark_builder_mock.appName.assert_called_once_with(
        "establishments-lakehouse"
    )


def test_creates_and_stops_spark_session(
    spark_builder_mock: MagicMock,
    spark_session_mock: MagicMock,
) -> None:
    with spark_session("establishments-lakehouse") as created_session:
        assert created_session is spark_session_mock

    spark_builder_mock.getOrCreate.assert_called_once_with()
    spark_session_mock.stop.assert_called_once_with()


@pytest.mark.usefixtures("spark_builder_mock")
def test_propagates_stop_failure_after_pipeline_success(
    spark_session_mock: MagicMock,
) -> None:
    stop_failure = RuntimeError("stop failed")
    spark_session_mock.stop.side_effect = stop_failure

    with pytest.raises(RuntimeError) as raised_failure:
        with spark_session("establishments-lakehouse"):
            pass

    assert raised_failure.value is stop_failure
    spark_session_mock.stop.assert_called_once_with()


@pytest.mark.usefixtures("spark_builder_mock")
def test_stops_spark_session_after_pipeline_failure(
    spark_session_mock: MagicMock,
) -> None:
    with (
        pytest.raises(RuntimeError, match="pipeline failed"),
        spark_session("establishments-lakehouse"),
    ):
        raise RuntimeError("pipeline failed")

    spark_session_mock.stop.assert_called_once_with()


@pytest.mark.usefixtures("spark_builder_mock")
def test_preserves_pipeline_failure_when_stop_also_fails(
    spark_session_mock: MagicMock,
) -> None:
    pipeline_failure = RuntimeError("pipeline failed")
    spark_session_mock.stop.side_effect = RuntimeError("stop failed")

    with (
        pytest.raises(RuntimeError) as raised_failure,
        spark_session("establishments-lakehouse"),
    ):
        raise pipeline_failure

    assert raised_failure.value is pipeline_failure
    spark_session_mock.stop.assert_called_once_with()


def test_rejects_non_string_application_name(
    spark_builder_mock: MagicMock,
) -> None:
    with (
        pytest.raises(TypeError, match="Application name must be a string"),
        spark_session(None),  # type: ignore[arg-type]
    ):
        pass

    spark_builder_mock.appName.assert_not_called()
    spark_builder_mock.getOrCreate.assert_not_called()


def test_rejects_empty_application_name(
    spark_builder_mock: MagicMock,
) -> None:
    with (
        pytest.raises(ValueError, match="Application name cannot be empty"),
        spark_session(" "),
    ):
        pass

    spark_builder_mock.appName.assert_not_called()
    spark_builder_mock.getOrCreate.assert_not_called()
