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
                subdag = subdag_factory('parent_dag','subdag_group_id',default_args),
                # mode - sub operator wont keep a worker slot forever util its completion. 
                # it willrelease the worker slot perodically so that if there is any other tasks to execute
                # then those tasks will be able to triggered and then   the sub operator will take a 
                # worker slot again after that. 
                mode = 'reschedule' ,
                # timeout - you are going to end up with a failure if your sub operator is taking longer
                # thank 5 mints to completes to complete 
                timeout = 60 * 5,  #5 mints
                # pool - the allocated pool will be repected by the suboperator and not the tasks 
                # that are in your sub dag, inside the tasks won't be executed in the pool.
                # instead of using pool here we can use in subdag.py default_args
                
                # pool = "training_ml"
        )
        

        end = BashOperator(
                task_id = 'end',
                bash_command = "echo 'end' "
        )

        start >> [subdag_group_id,subdag_group_id_2,subdag_group_id_3] >> end 