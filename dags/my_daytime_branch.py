from airflow.models import DAG  #type:ignore
from datetime import datetime,time
from airflow.operators.datetime import BranchDateTimeOperator #type:ignore
from airfow.operators.dummy import DummyOperator #type:ignore

default_args = {
        'start_date' : datetime(25,12,20),
        'schedule_interval' : '@daily'
}
with DAG(
        dag_id = 'my_datetime_dag',
        default_args = default_args,
        catchup = False
) as dag:
        
        is_in_time_frame = BranchDateTimeOperator(
                task_id = 'is_in_time_frame',
                follow_task_ids_true = ['move_forward'],
                follow_task_ids_false = ['end'],
                target_lower = time(10,0,0),
                target_upper = time(11,0,0)
                # Default it consider current date but, 
                # if any backdated run is present that time it will fail 
                # better we should use excecution date.
                # Dag will run based on the execution dt
                use_task_execution_dt = True
        )

        move_forward = DummyOperator(
                task_id = 'move_forward'
        )

        end = DummyOperator(
                task_id= 'end'
        )

        is_in_time_frame >> move_forward >> end
