from airflow import DAG   #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator  #type:ignore
from airflow.operators.python import PythonOperator #type:ignore

def _my_func(execution_date):
        if execution_date.day == 5:
                raise ValueError("Error")

def _sla_miss(dag,task_list,blocking_task_list,slas,blocking_tis):
        print(f"Dag id {dag} for SLA {slas}")

with DAG(
        dag_id = 'DownStream',
        start_date = datetime(25,12,10),
        schedule_interval = '*/2 * * * *',  #2 mints  airflow > admin > sla misses
        sla_miss_callback = _sla_miss,
        catchup = False
) as dag:
        task_a = BashOperator(
                task_id = 'task_a',
                bash_command = "echo Task A && sleep 10",
                sla = timedelta(seconds=5)
        )

        task_b = BashOperator(
                task_id = 'task_b',
                bash_command = "echo Task B && exit 0",
                retries = 3 ,
                retry_delay = timedelta(seconds = 10)
        )

        task_c = PythonOperator(
                task_id = 'task_c',
                python_callable = _my_func,
                depends_on_past = True
        )

        task_a >> task_b >> task_c