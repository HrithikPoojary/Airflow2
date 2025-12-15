from airflow import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore

with DAG(
        dag_id = 'my_dag',
        start_date = datetime(2025,1,1),
        schedule_interval = '@daily',
        catchup = False
         ) as dag :
        
        task_a = BashOperator(
                task_id = "taks_a",
                bash_command = "echo 'Task A'"
        )

        task_c = BashOperator(
                task_id = "taks_c",
                bash_command = "echo 'Task C'"
        )

        task_b = BashOperator(
                task_id = "task_b",
                bash_command = "echo 'Task B'"
        )

        task_a >> task_c >>task_b