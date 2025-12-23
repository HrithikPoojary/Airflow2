from airflow import DAG #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator   #type:ignore
from airflow.operators.python import PythonOperator  #type:ignore
from airflow.helpers import crosstream             #type:ignore

def _extract_a_success(context):
        print(context)
        print(f"Luffy's dag {context['dag']}")

def _extract_b_failure(context):
        print(context)
        print(f"Exception occurred when {context['exception']}")

def _my_func(execution_date):
        if execution_date.day == 5:
                raise ValueError("Error")

with DAG(
        dag_id = 'Xcom',
        start_date = datetime(25,12,15),
        schedule_interval = "@daily",
        catchup = False
) as dag:
        
        extract_a = BashOperator(
                owner = 'Luffy',
                task_id = 'extract_a',
                bash_command = "echo 'Task_a' && sleep 5",
                wait_for_downstream = True,
                excecution_timeout = timedelta(seconds = 10),
                on_success_callback = _extract_a_success 
        )

        extract_b = BashOperator(
                owner = "Zoro",
                task_id = 'extract_a',
                bash_command = "echo 'Task b' and sleep 5",
                on_failure_callback = _extract_b_failure
        )

        process_a = BashOperator(
                owner = "Nami",
                task_id = 'process_a',
                bash_command = "echo 'Process a' and sleep 5",
                pool = "process_task"
        )

        process_b = BashOperator(
                owner = "Ussop",
                task_id = 'process_b',
                bash_command = "echo 'Process b' and sleep 5",
                pool = "process_task"
        )

        process_c = BashOperator(
                owner = "Sanji",
                task_id = 'process_c',
                bash_command = "echo 'Process c' and sleep 5",
                pool = "process_task"
        )

        store = PythonOperator(
                task_id = 'store',
                python_callable = _my_func
        )

        crosstream([extract_a,extract_b], [process_a,process_b,process_c])
        [process_a,process_b,process_c] >> store

