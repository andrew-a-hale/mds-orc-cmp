import os
import sys
import time
from datetime import datetime

import yaml
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.append(f"{os.getenv('AIRFLOW_HOME')}/dags/project")
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

    # validate_extract = PythonOperator(
    #     dag=dag,
    #     task_id=f"validate_extract_{country}",
    #     python_callable=tasks.validate_extract,
    #     op_args=[country],
    # )

    load = PythonOperator(
        dag=dag,
        task_id=f"load_{country}",
        python_callable=tasks.load,
        op_args=[CONFIG, country],
    )

    # validate_load = PythonOperator(
    #     dag=dag,
    #     task_id=f"validate_load_{country}",
    #     python_callable=tasks.validate_load,
    #     op_args=[CONFIG, country],
    # )

    # tasks_list.append(extract >> validate_extract >> load >> validate_load)
    tasks_list.append(extract >> load)

transform = PythonOperator(
    dag=dag,
    task_id=f"transform",
    python_callable=tasks.transform,
    op_args=[CONFIG],
)

# validate_transform = PythonOperator(
#     dag=dag,
#     task_id=f"validate_transform",
#     python_callable=tasks.validate_transform,
#     op_args=[CONFIG],
# )

# dag
tasks_list >> transform

if __name__ == "__main__":
    dag.test()
