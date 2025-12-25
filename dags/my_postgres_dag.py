from airflow.models import DAG  # type:ignore
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator #type:ignore
from airflow.operators.python import PythonOperator #type:ignore

class CustomPythonOperator(PostgresOperator):

        template_fields = ("sql","parameters")

def _my_task():
        return 'Sanji'

with DAG(
        dag_id = 'my_postgres_dag',
        start_date = datetime(25,12,20),
        schedule_interval = "@daily",
        catchup = False
) as dag:
        
        create_table = PostgresOperator(
                task_id = 'create_table',
                postgres_conn_id = "postgres",
                sql = "sql/CREATE_TABLE_MY_TABLE.sql"
        )

        
        my_task = PythonOperator(
                task_id = 'my_task',
                python_callable = _my_task
        )

        insert_row = CustomPythonOperator(
                task_id = 'insert_row',
                postgres_conn_id = "postgres",
                sql = [ "sql/INSERT_TABLE_MY_TABLE.sql",
                       'select * from my_table;' 
                ],
                # We cannot dynamically pass parameter here 
                # So we can customize python operator
                parameters  = {
                        'piratename' : '{{ti.xcom_pull(task_ids = ["my_task"])}}'
                }
        )
