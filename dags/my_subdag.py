from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.subdag import SubDagOperator #type:ignore
from dags.subdag import subdag_factory

default_args = {
        'schedule_interval' : "daily",
        'start_date' : datetime(25,12,20)
}

with DAG(
        dag_id = 'parent_dag',
        default_args = default_args,
        catchup = False
) as dag:

        start = BashOperator(
                task_id = 'start',
                bash_command = "echo 'start' "
        )
        subdag_group_id = SubDagOperator(
                task_id = 'subdag_group_id',  # Subdag Id
                subdag = subdag_factory('parent_dag','subdag_group_id',default_args),
                mode = 'reschedule',
                # conf - we can additional information configaration setting  to your subject
                # we can use below output file in subdag run.
                conf = {'output':'opt/airflow.ml'},
                # suppose all task in subdag skipped and end task will execute anyway - default false
                # true all tasks skipped then end task will be skipped.
                propogate_skipped_state = True
        )
        

        end = BashOperator(
                task_id = 'end',
                bash_command = "echo 'end' "
        )

        start >> [subdag_group_id] >> end 