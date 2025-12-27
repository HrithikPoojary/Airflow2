# if you run manually and after that if run using TiggerDagRunOperator automaticaly you get some error.

from airflow.models import DAG  #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore

default_args = {
        'start_date' : datetime(25,12,20),
        'schedule_interval' : None
}
with DAG(
        dag_id = 'my_datetime_dag',
        default_args = default_args,
        catchup = False
) as dag:
        
        task = BashOperator(
                task_id = 'task',
                bash_command = "echo {{dag_run.conf['path']}}"
        )
        
