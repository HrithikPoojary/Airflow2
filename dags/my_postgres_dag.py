from airflow.models import DAG  # type:ignore
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator #type:ignore

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

        insert_row = PostgresOperator(
                task_id = 'insert_row',
                postgres_conn_id = "postgres",
                #multiple requests
                #for select we get only we get affected rows : 2
                #No xcom for select statement
                sql = [ "sql/INSERT_TABLE_MY_TABLE.sql",
                       'select * from my_table;' 
                ]
                # for first script
                        #Running statement: insert into my_table values('Zoro'), parameters: None
                        #Rows affected: 1
                # for second script
                        #Running statement: select * from my_table;, parameters: None
                        #Rows affected: 2
        )