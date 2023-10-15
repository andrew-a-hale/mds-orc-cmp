import time
from datetime import datetime
import yaml
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys

sys.path.append(f"{os.getenv('AIRFLOW_HOME')}/dags/project")
from project.salaries import tasks


PATH = os.path.dirname(__file__)
DBT_DIR = f"{PATH}/dbt"
CONFIG = yaml.safe_load(open(f"{PATH}/config.yml"))

dag = DAG(
    dag_id=f"{CONFIG.get('dag_id')}_{CONFIG.get('version')}",
    schedule=CONFIG.get("schedule"),
    start_date=datetime.fromisoformat(CONFIG.get("start_datetime_string")),
    catchup=CONFIG.get("catchup"),
    max_active_runs=CONFIG.get("max_active_runs"),
    default_args=CONFIG,
)

# create tasks
countries = CONFIG.get("countries")
tasks_list = []
for country in countries:
    extract = PythonOperator(
        dag=dag,
        task_id=f"extract_{country}",
        python_callable=tasks.extract,
        op_args=[CONFIG, country],
    )

    validate_extract = PythonOperator(
        dag=dag,
        task_id=f"validate_extract_{country}",
        python_callable=tasks.validate_extract,
        op_args=[country],
    )

    loaded_at = int(time.time())
    vars = f"csv_file: {CONFIG.get('datalake')}/{country}/salaries.csv"
    load = BashOperator(
        dag=dag,
        task_id=f"load_{country}",
        bash_command=f"dbt run --model salaries --vars '{vars}'",
    )

    vars = f"{{country: {country}, loaded_at: {loaded_at}}}"    
    validate_load = BashOperator(
        dag=dag,
        task_id=f"validate_load_{country}",
        bash_command=f"dbt test --model salaries --vars '{vars}'",
    )

    tasks_list.append(extract >> validate_extract >> load >> validate_load)

loaded_at = int(time.time())
transform = BashOperator(
    dag=dag,
    task_id=f"transform",
    bash_command=f"dbt run --model agg",
)

validate_transform = BashOperator(
    dag=dag,
    task_id=f"validate_transform",
    bash_command=f"dbt test --model agg --vars 'loaded_at: {loaded_at}'",
)

# dag
tasks_list >> transform >> validate_transform

if __name__ == "__main__":
    dag.test()
