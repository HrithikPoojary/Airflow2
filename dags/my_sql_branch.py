from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator #type:ignore
from airflow.operators.sql import BranchSQLOperator #type:ignore
from airflow.operators.dummy import DummyOperator #type:ignore

default_args ={
        'start_date' : datetime(25,12,20) 
}

with DAG(
        dag_id = 'my_sql_dag',
        schedule_interval = "@daily",
        default_args = default_args,
        catchup = False
) as dag:
        
        create_table = PostgresOperator(
                task_id = 'create_table',
                postgres_conn_id = "postgres",
                sql = 'sql/CREATE_TABLE_PARTNERS.sql'
        )

        insert_into = PostgresOperator(
                task_id = 'insert_into',
                postgres_conn_id = 'postgres',
                sql = 'sql/INSERT_INTO_PARTNERS.sql'
        )
        # Sql return True or False
        # If integer zero = false greater than zero true (0 < )
        # If string 1 or 0 ,yes or no  (preffered to use interger or boolean)
        table_name = "partners"
        choose_task = BranchSQLOperator(
                task_id = 'choose_task',
                sql = f"select count(*) from {table_name} where partner_status = TRUE;",
                follow_task_ids_if_true = ['process'],
                follow_task_ids_if_false = ['notif_email','notif_slack'],
                conn_id = 'postgres' 
        )

        process  = DummyOperator(
                task_id = 'process'
        )

        notif_email  = DummyOperator(
                task_id = 'notif_email'
        )

        notif_slack  = DummyOperator(
                task_id = 'notif_slack'
        )

        create_table >> insert_into >> choose_task >> [process,notif_email,notif_slack]