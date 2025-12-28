from airflow.models import DAG ##type:ignore
from datetime import datetime
from airflow.operators.dummy import DummyOperator #type:ignore
from airflow.operators.python import ShortCircuitOperator #type:ignore

def _is_monday(execution_date):
        return execution_date.weekday() == 0 # Monday

with DAG(
        dag_id= 'Short_Circuit',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:
        task_a = DummyOperator(
                task_id = 'task_a'
        )

        task_b = DummyOperator(
                task_id = 'task_b'
        )

        task_c = DummyOperator(
                task_id = 'task_c'
        )

        is_monday = ShortCircuitOperator(
                task_id = 'is_monday',
                # If python functions return false then the all downstream tasks of is weekly of the
                # -short circuit operator will be skipped.
                # if you want run specific tasks like last task we can use trigger rule and BranchPythonOperator
                python_callable = _is_monday
        )

        task_d = DummyOperator(
                task_id = 'task_d'
        )

        task_a >> task_b >> task_c >> is_monday >> task_d
