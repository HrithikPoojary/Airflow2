from airflow.models import DAG ,Variable #type:ignore
from airflow.decorators import task #type:ignore
from datetime import datetime
from typing import Dict
from airflow.operators.python import PythonOperator #type:ignore

#default task_id = pythonfunction name
@task( 
        task_id = 'task_a'
     )
def process(my_var):
        print(my_var)
        print(f"{my_var['path']} / {my_var['filename']}")

with DAG(
        dag_id = 'my_python_dag',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:

        process(Variable.get("my_setting",deserialize_json=True))   
