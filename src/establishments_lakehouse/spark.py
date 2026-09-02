import logging
from collections.abc import Generator
from contextlib import contextmanager

from pyspark.sql import SparkSession


_LOGGER = logging.getLogger(__name__)


@contextmanager
def spark_session(
    application_name: str,
) -> Generator[SparkSession, None, None]:
    normalized_name = _require_application_name(application_name)
    session = (
        SparkSession.builder
        .appName(normalized_name)
        .getOrCreate()
    )

    try:
        yield session
    except BaseException:
        _stop_preserving_failure(session)
        raise

    session.stop()


def _stop_preserving_failure(session: SparkSession) -> None:
    try:
        session.stop()
    except Exception:
        _LOGGER.exception(
            "Could not stop SparkSession after pipeline failure."
        )


def _require_application_name(application_name: object) -> str:
    if not isinstance(application_name, str):
        raise TypeError("Application name must be a string.")

    normalized_name = application_name.strip()
    if not normalized_name:
        raise ValueError("Application name cannot be empty.")

    return normalized_name
