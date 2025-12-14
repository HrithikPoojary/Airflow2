from airflow import DAG #type:ignore
from datetime import datetime
from airflow.operators.dummy import DummyOperator #type:ignore

with DAG(
        dag_id = 'my_dag',
        start_date = datetime(2025,1,1),
        schedule_interval = '@daily',
        catchup = False
         ) as dag :
        
        task_a = DummyOperator(
                task_id = "taks_a"
        )

        task_c = DummyOperator(
                task_id = "taks_c"
        )

        task_b = DummyOperator(
                task_id = "task_b"
        )

        task_a >> task_c >>task_b