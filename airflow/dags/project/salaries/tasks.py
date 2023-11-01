def extract(config, country):
    import csv
    import json
    import os
    import time

    import requests
    from airflow.operators.python import get_current_context

    req = requests.get(
        f"https://api.teleport.org/api/countries/iso_alpha2:{country}/salaries/"
    )
    req.raise_for_status()

    path = f"{config.get('datalake')}/{country}"
    if not os.path.exists(path):
        os.mkdir(path)

    loaded_at = int(time.time())
    with open(f"{path}/salaries.csv", mode="w") as file:
        data = json.loads(req.content.decode())
        csv_writer = csv.writer(file, delimiter=",")
        csv_writer.writerow(
            ["country", "id", "title", "p25", "p50", "p75", "loaded_at"]
        )
        for el in data["salaries"]:
            row = [
                country,
                el.get("job").get("id"),
                el.get("job").get("title"),
                el.get("salary_percentiles").get("percentile_25"),
                el.get("salary_percentiles").get("percentile_50"),
                el.get("salary_percentiles").get("percentile_75"),
                loaded_at,
            ]

            csv_writer.writerow(row)

        ctx = get_current_context()
        ctx["ti"].xcom_push("loaded_at", loaded_at)
        ctx["ti"].xcom_push(key="output_filename", value=f"{file.name}")


def validate_extract(country):
    import os

    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    filename = ctx["ti"].xcom_pull(task_ids=f"extract_{country}", key="output_filename")

    assert os.path.exists(filename)
    assert os.path.getsize(filename) > 0


def load(config, country):
    import duckdb
    import time
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    filename = ctx["ti"].xcom_pull(task_ids=f"extract_{country}", key="output_filename")

    conn = None
    while conn is None:
        try:
            conn = duckdb.connect(config.get("db_conn"))
        except duckdb.duckdb.IOException:
            time.sleep(0.5)
    cur = conn.cursor()
    tables = [x[0] for x in cur.execute("SHOW TABLES").fetchall()]
    if "salaries" in tables:
        cur.execute(f"COPY salaries FROM '{filename}' (AUTO_DETECT true)")
    else:
        cur.execute(
            f"CREATE TABLE salaries AS SELECT * FROM read_csv_auto('{filename}')"
        )
    conn.commit()
    conn.close()


def validate_load(config, country):
    import duckdb
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    loaded_at = ctx["ti"].xcom_pull(task_ids=f"extract_{country}", key="loaded_at")

    conn = duckdb.connect(database=config.get("db_conn"), read_only=True)
    cur = conn.cursor()
    rows = cur.execute(
        f"SELECT COUNT(*) FROM salaries WHERE loaded_at = {loaded_at}"
    ).fetchone()
    conn.commit()
    conn.close()

    if rows and rows[0] > 0:
        return True

    raise AirflowFailException("failed load")


def transform(config):
    import duckdb
    import time
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    loaded_at = int(time.time())
    ctx["ti"].xcom_push("loaded_at", loaded_at)

    conn = None
    while conn is None:
        try:
            conn = duckdb.connect(config.get("db_conn"))
        except duckdb.duckdb.IOException:
            time.sleep(0.5)
    cur = conn.cursor()
    sql = f"""\
CREATE TABLE IF NOT EXISTS agg (country VARCHAR, a25 DOUBLE, a50 DOUBLE, a75 DOUBLE, loaded_at BIGINT);
INSERT INTO agg (country, a25, a50, a75, loaded_at)
SELECT
    country,
    AVG(p25) AS a25,
    AVG(p50) AS a50,
    AVG(p75) AS a75,
    {loaded_at} AS loaded_at
FROM salaries
GROUP BY country;"""
    cur.execute(sql).fetchone()
    conn.commit()
    conn.close()


def validate_transform(config):
    import duckdb
    from airflow.exceptions import AirflowFailException
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    loaded_at = ctx["ti"].xcom_pull(task_ids=f"transform", key="loaded_at")

    conn = duckdb.connect(database=config.get("db_conn"), read_only=True)
    cur = conn.cursor()
    sql = f"""\
SELECT COUNT(*)
FROM agg
WHERE loaded_at = {loaded_at}"""
    rows = cur.execute(sql).fetchone()
    conn.commit()
    conn.close()

    if rows and rows[0] > 0:
        return True

    raise AirflowFailException("failed transform")
