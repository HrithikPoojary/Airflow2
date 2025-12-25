from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.dummy import DummyOperator #type:ignore
from airflow.operators.python import BranchPythonOperator #type:ignore

# In this function we return next run task
def _check_accuaracy():
        accuracy = 0.16
        if accuracy > 0.15 :
                return 'accurate'
        else:
                return 'inaccurate'

with DAG(
        dag_id = 'branch_operator_dag',
        start_date = datetime(25,12,20),
        schedule_interval = "@daily",
        catchup = False
        )  as dag:

        training_ml = DummyOperator(task_id = 'training_ml')

        check_accuracy = BranchPythonOperator(
                task_id = 'check_accuracy',
                python_callable = _check_accuaracy
        )

        accurate = DummyOperator(task_id = 'accurate')

        inaccurate = DummyOperator(task_id = 'inaccute')

        training_ml >> check_accuracy >> [accurate,inaccurate]