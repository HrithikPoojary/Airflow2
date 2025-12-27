from airflow.models import DAG  #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore
from airflow.sensors.external_task import ExternalTaskSensor #type:ignore

default_args = {
        'start_date' : datetime(25,12,20),
        'schedule_interval' : '@daily'
}
with DAG(
        dag_id = 'my_datetime_dag',
        default_args = default_args,
        catchup = False
) as dag:
        # Important - you have to make sure that the execution dates between the dag where- 
        # -the external task sensor is and the dag where the task you are waiting for are equal.
        # otherwise external task sensor will run forever.
        # parent dag execution date == external task sensor execution date 

        #Airflow >> my_datetime_dag >> log -> Poking for parent_dag.end on (Execution_dt)2025-12-26T17:32:07.479372+00:00 ... 
        waiting_for_task = ExternalTaskSensor(
                task_id = 'waiting_for_task',
                external_dag_id = 'parent_dag',
                external_task_id = 'end'
        )
        
        training_ml = BashOperator(
                task_id = 'task',
                bash_command = " training_ml "
        )

        waiting_for_task >> training_ml
        
