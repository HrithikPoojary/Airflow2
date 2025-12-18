from airflow import DAG #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator #type:ignore


with DAG(
        dag_id = 'my_dag_v1_0_0_0',
        start_date = datetime(21,1,1),
        schedule_interval = '@daily',
        catchup = False
         ) as dag :
        
        task_a = BashOperator(
                owner = 'Macr',
                task_id = "taks_a",
                bash_command = "echo 'Task A'"
        )

        task_b = BashOperator(
                owner = 'Luffy',
                task_id = "task_b",
                retries = 3,
                retry_delay = timedelta(seconds = 10),
                bash_command = "sleep 5 && exit 1"
        )

        task_a >> task_b