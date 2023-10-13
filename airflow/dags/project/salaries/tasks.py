def extract(config, country):
    import os
    import requests
    from airflow.operators.python import get_current_context

    req = requests.get(
        f"https://api.teleport.org/api/countries/iso_alpha2:{country}/salaries/"
    )
    req.raise_for_status()

    file = open(f"{config.get('datalake')}/{country}/salaries.json", mode="w")
    file.write(req.content.decode())

    ctx = get_current_context()
    ctx["ti"].xcom_push(key="output_filename", value=f"{os.path.dirname(file.name)}/{file.name}")
    file.close()


def validate_extract(filename):
    import os

    with open(filename) as file:
        assert os.path.exists(file)
        assert os.path.getsize(file) > 0


def load(config, country):
    import json
    import sqlite3
    import time
    from airflow.operators.python import get_current_context

    salaries_json = open(f"{config.get('datalake')}/{country}/salaries.json").read()
    salaries = json.loads(salaries_json)["salaries"]

    rows = []
    for salary in salaries:
        rows.append(
            [
                country,
                salary["job"]["id"],
                salary["job"]["title"],
                salary["salary_percentiles"]["percentile_25"],
                salary["salary_percentiles"]["percentile_50"],
                salary["salary_percentiles"]["percentile_75"],
            ]
        )

    conn = sqlite3.connect(f"{config.get('datalake')}/salaries.db")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS salaries (country TEXT, id TEXT, title TEXT, p25 NUM, p50 NUM, p75 NUM, loaded_at INTEGER)"
    )
    
    ctx = get_current_context()
    now = int(time.time())
    ctx["ti"].xcom_push("loaded_at", now)

    cur.executemany(f"INSERT INTO salaries VALUES (?, ?, ?, ?, ?, ?, {now}))", rows)
    conn.commit()
    conn.close()


def transform(config):
    import sqlite3
    import time
    from airflow.operators.python import get_current_context

    conn = sqlite3.connect(f"{config.get('datalake')}/salaries.db")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS agg (country TEXT, a25 NUM, a50 NUM, a75 NUM, loaded_at INTEGER)"
    )

    loaded_at = int(time.time())
    ctx = get_current_context()
    now = int(time.time())
    ctx["ti"].xcom_push("loaded_at", now)

    cur.execute(
        f"INSERT INTO agg SELECT country, AVG(p25), AVG(p50), AVG(75), {loaded_at} FROM salaries GROUP BY country"
    )
    conn.commit()
    conn.close()
