from airflow import DAG   #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator  #type:ignore
from airflow.operators.python import PythonOperator #type:ignore

'''
on_success_callback
on_failure_callback
on_execute_callback
on_retry_callback
We will get context we can use it in the callable function
To track Airflow  >> logs
'''

def _my_func(execution_date):
        if execution_date.day == 5:
                raise ValueError("Error")
def _task_a_sucess(context):
        print(context)
        print(f"Luffy's Dag Run {context['dag']}")

def _task_b_failure(context):
        print(context)
        print(f"Failed due to {context['exception']}")

with DAG(
        dag_id = 'DownStream',
        start_date = datetime(25,12,10),
        schedule_interval = '@daily',  #2 mints  airflow > admin > sla misses
        catchup = False
) as dag:
        task_a = BashOperator(
                task_id = 'task_a',
                bash_command = "echo Task A && sleep 10",
                execution_timeout = timedelta(seconds=20),
                on_success_callback=_task_a_sucess
        )

        task_b = BashOperator(
                task_id = 'task_b',
                bash_command = "echo Task B && exit 1",
                retries = 3 ,
                retry_delay = timedelta(seconds = 10),
                on_failure_callback = _task_b_failure
        )

        task_c = PythonOperator(
                task_id = 'task_c',
                python_callable = _my_func,
                depends_on_past = True
        )

        task_a >> task_b >> task_c