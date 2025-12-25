from airflow.models import DAG ,Variable #type:ignore
from airflow.decorators import task #type:ignore
from datetime import datetime
from typing import Dict
from airflow.operators.bash import BashOperator #type:ignore
from Function.pythonFun import process


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
