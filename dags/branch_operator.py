from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.dummy import DummyOperator #type:ignore
from airflow.operators.python import BranchPythonOperator #type:ignore
import yaml #type:ignore

# In this function we return next run task
def _check_holidays(**context):
        with open ('dags/files/holidays_file.yml','r') as f:
                days_off = set(yaml.load(f,Loader = yaml.FullLoader))
                if context['ds'] not in days_off:
                        return 'process'
                else:
                        'stop'


with DAG(
        dag_id = 'branch_operator_dag',
        start_date = datetime(25,12,20),
        schedule_interval = "@daily",
        catchup = False
        )  as dag:

        check_holidays = BranchPythonOperator(
                task_id = 'check_holidays',
                python_callable = _check_holidays
        )

        process = DummyOperator(task_id = 'process')

        cleaning = DummyOperator(task_id = 'cleaning')

        stop = DummyOperator(task_id = 'stop')

        # Publish_ml task will be skipped because defualt trigger rule (all_success)
        # all_success -> All parent task should compplete.
        # to avoid that use trigger rule = none_failed_or_skipped
        check_holidays >> [process,stop]
        process >> cleaning