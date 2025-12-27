from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore

default_args = {
        'schedule_interval' : "daily"
}

with DAG(
        dag_id = 'parent_dag',
        start_date = datetime(25,12,20),
        default_args = default_args,
        catchup = False
) as dag:

        start = BashOperator(
                task_id = 'start',
                bash_command = "echo 'start' "
        )


        training_a = BashOperator(task_id = 'training_a',bash_command = "echo 'training_a' ")
        training_b = BashOperator(task_id = 'training_b',bash_command = "echo 'training_b' ")
        training_c = BashOperator(task_id = 'training_c',bash_command = "echo 'training_c' ")

        end = BashOperator(
                task_id = 'end',
                bash_command = "echo 'end' "
        )

        start >> [training_a,training_b,training_c] >> end 