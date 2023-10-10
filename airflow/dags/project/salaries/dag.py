from datetime import datetime
import yaml
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import os
from project.salaries import tasks

PATH = os.path.dirname(__file__)
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
    load = PythonOperator(
        dag=dag,
        task_id=f"load_{country}",
        python_callable=tasks.load,
        op_args=[CONFIG, country],
    )

    tasks_list.append(extract >> load)

transform = PythonOperator(
    dag=dag, task_id="transform", python_callable=tasks.transform, op_args=[CONFIG]
)

# dag
tasks_list >> transform
