from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG(dag_id="test_airflow1", start_date=datetime(2022, 1, 1), schedule_interval="5 0 * * *") as dag:
    first_task = BashOperator(task_id='firs_task', bash_command=f"python3 /opt/airflow/dags/test_dag1.py")
    second_task = BashOperator(task_id='second_task', bash_command=f"python3 /opt/airflow/dags/test_dag1.py")

    @task()
    def finaly_task():
        print("Rabotay plz")

    first_task >> second_task >> finaly_task()