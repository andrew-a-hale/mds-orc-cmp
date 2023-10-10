def extract(config, country):
    import requests

    req = requests.get(
        f"https://api.teleport.org/api/countries/iso_alpha2:{country}/salaries/"
    )
    req.raise_for_status()

    file = open(f"{config.get('datalake')}/salaries.json", mode="w")
    file.write(req.content.decode())


def load(config, country):
    import json
    import sqlite3

    salaries_json = open(f"{config.get('datalake')}/salaries.json").read()
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
        "CREATE TABLE IF NOT EXISTS salaries (country TEXT, id TEXT, title TEXT, p25 NUM, p50 NUM, p75 NUM)"
    )
    cur.executemany("INSERT INTO salaries VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def transform(config):
    import sqlite3

    conn = sqlite3.connect(f"{config.get('datalake')}/salaries.db")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS agg (country TEXT, a25 NUM, a50 NUM, a75 NUM)"
    )
    cur.execute(
        "INSERT INTO agg SELECT country, AVG(p25), AVG(p50), AVG(75) FROM salaries GROUP BY country"
    )
    conn.commit()
    conn.close()
