from airflow.models import DAG  #type:ignore
from datetime import datetime
from airflow.operators.python import PythonOperator #type:ignore

# op_args = Order matter
# op_kwargs = order doesn't matter
# Normal way 

path = 'gobal/local/airflow'
filename = 'insta.csv'

def _process(path,filename):
     print(f"{path}/{filename}")

with DAG(
        dag_id = 'my_python_dag',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:

     task_a = PythonOperator(
          task_id = 'task_a',
          python_callable = _process,
          op_kwargs = {
             'path'    :  path, 
             'filename':  filename 
          }
     )   
