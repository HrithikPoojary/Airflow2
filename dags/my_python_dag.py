from airflow.models import DAG ,Variable #type:ignore
from airflow.decorators import task #type:ignore
from datetime import datetime
from typing import Dict
from airflow.operators.python import get_current_context #type:ignore
from airflow.operators.bash import BashOperator #type:ignore

#default task_id = pythonfunction name
@task( 
        task_id = 'task_a'
)
def process(my_var):
        context = get_current_context()
        print(context)
        print(f"{my_var['path']} / {my_var['filename']}")
        return 123

with DAG(
        dag_id = 'my_python_dag',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:
        
        store = BashOperator(
                task_id = 'store',
                bash_command = "echo 'Store'"
        )

        process(Variable.get("my_setting",deserialize_json=True)) >> store  
