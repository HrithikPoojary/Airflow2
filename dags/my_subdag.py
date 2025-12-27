from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.subdag import SubDagOperator #type:ignore
from dags.subdag import subdag_factory
from task_groups import task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type:ignore
default_args = {
        'start_date' : datetime(25,12,20),
        'schedule_interval' : "@daily"
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

        grouping_task_group = task_group()

        end = BashOperator(
                task_id = 'end',
                bash_command = "echo 'end' "
        )

        start >> grouping_task_group >> end 