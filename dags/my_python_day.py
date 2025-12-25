from airflow.models import DAG ,Variable #type:ignore
from datetime import datetime
from typing import Dict
from airflow.operators.python import PythonOperator #type:ignore


def _process(path,filename,**context : Dict):
     print(context)
     print(f"{path}/{filename} - {context['ds']}")
     # or we can directly pass def _process(path,filename,ds):
     # Not allowed def _process(**context ,path,filename):

with DAG(
        dag_id = 'my_python_dag',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:

     task_a = PythonOperator(
          task_id = 'task_a',
          python_callable = _process,
          # we are making twice connection to the metadatabase
          #using '{{ var.value.path}}' and '{{var.value.filepath}}' 
          # we can avoid using this
          op_kwargs = Variable.get("my_setting",deserialize_json=True)
     )   
