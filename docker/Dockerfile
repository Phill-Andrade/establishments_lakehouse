ARG PLATFORM_IMAGE=hadoop_platform

FROM ${PLATFORM_IMAGE}

WORKDIR /opt/pipeline

ENV PYTHONPATH=/opt/pipeline/src

COPY src/ /opt/pipeline/src/

WORKDIR /opt/pipeline/src

RUN /usr/bin/python3 -m zipfile -c \
    /opt/pipeline/establishments_lakehouse.zip \
    establishments_lakehouse

WORKDIR /opt/pipeline
