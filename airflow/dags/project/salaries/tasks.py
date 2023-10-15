def extract(config, country):
    import json
    import csv
    import os
    import requests
    from airflow.operators.python import get_current_context

    req = requests.get(
        f"https://api.teleport.org/api/countries/iso_alpha2:{country}/salaries/"
    )
    req.raise_for_status()

    path = f"{config.get('datalake')}/{country}"
    if not os.path.exists(path):
        os.mkdir(path)

    with open(f"{path}/salaries.csv", mode="w") as file:
        data = json.loads(req.content.decode())
        for el in data["salaries"]:
            row = [
                el.get("job").get("id"),
                el.get("job").get("title"),
                el.get("salary_percentiles").get("percentile_25"),
                el.get("salary_percentiles").get("percentile_50"),
                el.get("salary_percentiles").get("percentile_75"),
            ]

            csv_writer = csv.writer(file, delimiter=",")
            csv_writer.writerow(row)

        ctx = get_current_context()
        ctx["ti"].xcom_push(key="output_filename", value=f"{file.name}")


def validate_extract(country):
    import os
    from airflow.operators.python import get_current_context

    ctx = get_current_context()
    filename = ctx["ti"].xcom_pull(task_ids=f"extract_{country}", key="output_filename")

    assert os.path.exists(filename)
    assert os.path.getsize(filename) > 0
