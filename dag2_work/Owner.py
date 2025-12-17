from airflow import DAG,task  #type:ignore
from airflow.operators.bash import BashOperator  #type:ignore
from datetime import datetime

default_args = {
        'owner' : 'airflow'
}

with DAG(
        dag_id = 'my_dag',
        default_args = default_args,
        schedule_interval = '@daily',
        start_time = datetime(25,1,1),
        catchup = False
) as dag:
        task_a = BashOperator(
                owner = 'Luffy',
                task_id = 'task_a',
                bash_command = 'echo hello world'
        )
        task_b = BashOperator(
                owner = 'Zoro',
                task_id = 'task_b',
                bash_command = 'echo Hello Planet'
        )

        task_a >> task_b