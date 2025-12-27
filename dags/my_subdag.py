from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.subdag import SubDagOperator #type:ignore
from dags.subdag import subdag_factory
from task_groups import task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type:ignore
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

        grouping_task_group = task_group()
        
        process_ml = TriggerDagRunOperator(
                task_id = 'process_ml',
                trigger_dag_id = 'my_datetime_dag', # this can be templated like {{var.value.variable_name}}
                # this allows you to pass additional information to the dag triggered by the trigger dag run operator
                # this path will be given to the dag triggered by the trigger dag run operator
                conf = {
                        'path' : 'opt/local/user'
                },
                 # same execution date of parent dag date context['ds']
                execution_date = "{{ ds }}",
                # if it is a false then you won't be rerun the past triggered dag run according to parent dag and target dag
                reset_dag_run = True,
                # this wait till target dag completes.
                wait_for_completion = True
        )


        end = BashOperator(
                task_id = 'end',
                bash_command = "echo 'end' "
        )

        start >> grouping_task_group >> process_ml >> end 