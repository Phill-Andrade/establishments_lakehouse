from collections.abc import Generator
from contextlib import contextmanager

from pyspark.sql import SparkSession


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
    finally:
        session.stop()


def _require_application_name(application_name: object) -> str:
    if not isinstance(application_name, str):
        raise TypeError("Application name must be a string.")

    if not application_name.strip():
        raise ValueError("Application name cannot be empty.")

    return application_name.strip()
