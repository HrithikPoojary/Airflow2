from airflow import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore

default_args = {
        'owner':'Marc'
}

with DAG(
        dag_id = 'my_dag_v1_0_0_1',
        start_date = datetime(2025,1,1),
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False
         ) as dag :
        
        task_a = BashOperator(
                task_id = "taks_a",
                bash_command = "echo 'Task A'"
        )

        task_b = BashOperator(
                task_id = "task_b",
                bash_command = "echo 'Task B'"
        )

        task_a >> task_b