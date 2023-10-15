ARG AIRFLOW_VERSION=2.7.2
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS base

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN apt-get update
RUN apt-get install -y git wget unzip

RUN wget https://github.com/duckdb/duckdb/releases/download/v0.9.1/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-amd64.zip

USER airflow
RUN pip install duckdb dbt-core dbt-duckdb

RUN mkdir local
RUN duckdb local/db.duckdb

ENV DBT_DIR /opt/airflow/dags/project/salaries/dbt
ENV DBT_PROFILES_DIR /opt/profiles
ENV DBT_LOG_PATH ${DBT_DIR}/logs

# Copy dbt code and the profiles.yml file to the image
COPY ./dags/project/salaries/dbt $DBT_DIR/
COPY ./dags/project/salaries/dbt/profiles.yml $DBT_PROFILES_DIR/profiles.yml

# Set working directory
WORKDIR $DBT_DIR
RUN sudo chmod -R 777 ${DBT_DIR}

# Run dbt
USER airflow
RUN dbt deps

FROM base AS ci
USER airflow
EXPOSE 5000

COPY --chown=airflow:root docker/entrypoint.sh /entrypoint-custom.sh
RUN chmod +x /entrypoint-custom.sh
COPY --chown=airflow:root docker/setup_mockdata.sh /setup_mockdata.sh
RUN chmod +x /setup_mockdata.sh

COPY --chown=airflow:root dags/ /opt/airflow/dags/

ENV PATH="${PATH}:/home/airflow/.local/bin"
ENTRYPOINT ["/entrypoint-custom.sh"]
CMD ["airflow", "--help"]
