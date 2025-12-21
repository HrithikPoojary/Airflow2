from airflow import DAG #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.python import PythonOperator #type:ignore

def _my_func(execution_date):
        if execution_date.day ==5 :
                raise ValueError("Error")

with DAG(
        dag_id = 'DownStream',
        start_date = datetime(25,12,10),
        schedule_interval = '@daily',
        catchup = True
) as dag:
        task_a = BashOperator(
                task_id = 'task_a',
                bash_command = "echo 'task A' && sleep 5",
                wait_for_downstream = True
        )

        task_b = BashOperator(
                task_id = 'task_b',
                retries = 3,
                retry_exponential_backoff = True,
                retry_delay = timedelta(seconds=5),
                bash_command = "echo 'task B' && exit  0"       
        )

        task_c = PythonOperator(
                task_id = 'task_c',
                python_callable = _my_func
        )

        task_a >> task_b >> task_c