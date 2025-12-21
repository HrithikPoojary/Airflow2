from airflow import DAG #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.python import PythonOperator #type:ignore
from airflow.utils.helpers import cross_downstream #type:ignore

def _my_func(execution_date):
        if execution_date.day == 5 :
                raise ValueError("Error")

with DAG(
        dag_id = 'DownStream',
        start_date = datetime(25,12,10),
        schedule_interval = '@daily',
        catchup = False
) as dag:
        extract_a = BashOperator(
                task_id = 'extract_a',
                bash_command = "echo 'task A' && sleep 5",
                wait_for_downstream = True
        )

        extract_b = BashOperator(
                task_id = 'extract_b',
                bash_command = "echo 'task A' && sleep 5",
                wait_for_downstream = True
        )

        process_a = BashOperator(
                task_id = 'process_a',
                retries = 3,
                retry_exponential_backoff = True,
                retry_delay = timedelta(seconds=5),
                bash_command = "echo 'task B' && sleep 20"       
        )

        process_b = BashOperator(
                task_id = 'process_b',
                retries = 3,
                retry_exponential_backoff = True,
                retry_delay = timedelta(seconds=5),
                bash_command = "echo 'task B' && sleep 20"       
        )

        process_c = BashOperator(
                task_id = 'process_c',
                retries = 3,
                retry_exponential_backoff = True,
                retry_delay = timedelta(seconds=5),
                bash_command = "echo 'task B' && sleep 20"       
        )

        store = PythonOperator(
                task_id = 'task_c',
                python_callable = _my_func,
                depends_on_past = True
        )

       # Error [extract_a,extract_b] >> [process_a,process_b,process_c] >> store

        cross_downstream([extract_a,extract_b] , [process_a,process_b,process_c])
        [process_a,process_b,process_c] >> store