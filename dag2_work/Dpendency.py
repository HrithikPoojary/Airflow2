from airflow import DAG   #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator  #type:ignore
from airflow.operators.python import PythonOperator #type:ignore

def _my_func(execution_dt):
        if execution_dt.day == 5 :
                raise ValueError("Error")

with DAG(
        dag_id = 'my_dag',
        start_date = datetime(25,12,1),
        schedule_interval = "@daily",
        catchup = True
) as dag:
        task_a = BashOperator(
                task_id = 'task_a',
                bash_command = "echo Task A && sleep 10"
        )

        task_b = BashOperator(
                task_id = 'task_b',
                bash_command = "echo Task B && exit 0",
                retries = 3 ,
                retry_delay = timedelta(seconds = 10),
                retry_exponentional_backoff = False
        )

        task_c = PythonOperator(
                task_id = 'task_c',
                python_callable = _my_func,
                depends_on_past = True
        )

        task_a >> task_b >> task_c