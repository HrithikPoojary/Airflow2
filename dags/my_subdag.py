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
        # - AIRFLOW__CORE__PARALLELISM=3  Only 3 task can be run at a time If you trigger this we will get deadlock issue
        subdag_group_id = SubDagOperator(
                task_id = 'subdag_group_id',  # Subdag Id
                subdag = subdag_factory('parent_dag','subdag_group_id',default_args)
        )

        subdag_group_id_2 = SubDagOperator(
                task_id = 'subdag_group_id_2',  # Subdag Id
                subdag = subdag_factory('parent_dag','subdag_group_id_2',default_args)
        )

        subdag_group_id_3 = SubDagOperator(
                task_id = 'subdag_group_id_3',  # Subdag Id
                subdag = subdag_factory('parent_dag','subdag_group_id_3',default_args)
        )
        

        end = BashOperator(
                task_id = 'end',
                bash_command = "echo 'end' "
        )

        start >> [subdag_group_id,subdag_group_id_2,subdag_group_id_3] >> end 