from airflow.models import DAG  #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator #type:ignore
from airflow.sensors.external_task import ExternalTaskSensor #type:ignore

default_args = {
        'start_date' : datetime(25,12,20)
}
with DAG(
        dag_id = 'my_datetime_dag',
        schedule_interval = '15 * * * *',   # 1:15,2:15,3:15 so on...
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
                external_task_id = 'end',
                # parent_dag = 1:10 and task sensor 1:15 difference is 5 mints (-5)
                execution_delta = timedelta(minutes = 5) 
        )
        
        training_ml = BashOperator(
                task_id = 'training_ml',
                bash_command = "echo 'training_ml' "
        )

        waiting_for_task >> training_ml
        
