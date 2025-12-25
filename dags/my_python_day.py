from airflow.models import DAG  #type:ignore
from datetime import datetime
from airflow.operators.python import PythonOperator #type:ignore
'''
docker ps
docker exec -it <container_id scheduler> /bin/bash
airflow tasks test <dag_id> <task_id> <execution_dt> 
'''

def _my_fun():
     print("Hello Task A")


with DAG(
        dag_id = 'my_python_dag',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:

     task_a = PythonOperator(
          task_id = 'task_a',
          python_callable = _my_fun
     )   
