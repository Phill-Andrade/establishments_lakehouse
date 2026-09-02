FROM hadoop_platform

WORKDIR /opt/pipeline

ENV PYTHONPATH=/opt/pipeline/src

COPY src/ /opt/pipeline/src/

WORKDIR /opt/pipeline/src

RUN /usr/bin/python3 -m zipfile -c \
    /opt/pipeline/establishments_lakehouse.zip \
    establishments_lakehouse

WORKDIR /opt/pipeline
