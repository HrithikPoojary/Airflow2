from airflow import DAG #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.python import PythonOperator #type:ignore

def _my_fun(execution_date):
        if execution_date.day == 5:       # Every Run it will increase the day namber 1,2,3,4,5 at 5 it will fail
                raise ValueError('Error')

with DAG(
        dag_id = 'my_dag_v1_0_0_0',
        start_date = datetime(25,12,1),
        schedule_interval = '@daily',
        dagrun_timeout = 40,    #seconds
        catchup = True
         ) as dag :

        task_a = BashOperator(
                owner = 'Macr',
                task_id = "taks_a",
                bash_command = "echo Task A && sleep 10"
        )
        #Task level 
        task_b = BashOperator(
                owner = 'Luffy',
                task_id = "task_b",
                bash_command = "echo task B && exit 0"
        )

        task_c = PythonOperator(
                task_id = 'task_c',
                python_callable = _my_fun,
                depends_on_past = True
        )


        task_a >> task_b >> task_c