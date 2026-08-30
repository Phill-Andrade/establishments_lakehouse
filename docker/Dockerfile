FROM hadoop_platform

WORKDIR /opt/pipeline

ENV PYTHONPATH=/opt/pipeline/src

COPY pyproject.toml poetry.lock /opt/pipeline/
COPY configs/ /opt/pipeline/configs/
COPY src/ /opt/pipeline/src/
