ARG AIRFLOW_VERSION=2.7.2
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS base

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER airflow
RUN pip install duckdb
