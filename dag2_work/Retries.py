from airflow import DAG  #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator  #type:ignore

with DAG(
        dag_id = 'Retry_dag',
        start_date = datetime(25,12,1),
        schedule_interval = '@daily',
        catchup = False
) as dag:
        task_a = BashOperator(
                task_id = 'task_a',
                bash_command = 'echoo task A '
        )

        task_b = BashOperator(
                task_id = 'task_b',
                bash_command = 'echo task B && exit 1'
        )

        task_a >> task_b