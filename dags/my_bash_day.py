from airflow.models import DAG   #type:ignore
from datetime import datetime
from airflow.operators.bash import BashOperator #type:ignore
with DAG(
        dag_id = 'my_bash_dag',
        start_date = datetime(25,12,20),
        schedule_interval = '@daily',
        catchup = False
) as dag:
        execute_command = BashOperator(
                task_id = 'execute_command',
                bash_command = "scripts/command.sh",
                do_xcom_push = False,
                # api_aws : ***
                # api_key , access_token , password ,secret ,authorization so on ..In that case values automatically hidden                env = {
                env = {        
                        "api_aws" : "{{var.value.api_key_aws}}"
                }
        )