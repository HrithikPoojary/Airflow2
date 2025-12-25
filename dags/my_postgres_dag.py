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
                #To create (postgres_conn_id)
                #Airflow > admin > connections
                #Conn Id = postgres
                #conn type = postgres
                #host = postgres
                #login = postgres
                #passeword = postgres
                #port = 5432
                postgres_conn_id = "postgres",
                sql = "create table my_table(table_value text not null , primary key (table_value));"
        )
        '''
        To connect Postgres
        docker ps
        docker exec -it <postgres container_id> /bin/bash
        root@ce37da20b8ad:/# > psql -Upostgres
        postgres=# select * from table_name;
        '''