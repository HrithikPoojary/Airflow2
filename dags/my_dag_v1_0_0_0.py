from airflow import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore


with DAG(
        dag_id = 'my_dag_v1_0_0_0',
        start_date = datetime.now(),
        schedule_interval = '*/1 * * * *',
        catchup = False
         ) as dag :
        
        task_a = BashOperator(
                owner = 'Macr',
                task_id = "taks_a",
                bash_command = "echo 'Task A'"
        )

        task_c = BashOperator(
                owner = 'Krishna',
                task_id = "taks_c",
                bash_command = "echo 'Task C'"
        )

        task_b = BashOperator(
                owner = 'Luffy',
                task_id = "task_b",
                bash_command = "echo 'Task B'"
        )

        task_a >> task_c >>task_b