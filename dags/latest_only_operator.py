from airflow.models import DAG #type:ignore
from airflow.utils.dates import days_ago #type:ignore
from airflow.operators.dummy import DummyOperator #type:ignore
from airflow.operators.latest_only import LatestOnlyOperator #type:ignore

with DAG(
        dag_id = 'latest_only_operator_dag',
        start_date = days_ago(3),  # 3 days ago
        schedule_interval = "@daily",
        catchup = True
) as dag:
        
        # It works if below condition satisfies then that is the latest dag run
        # current execution date (2021-01-01 00:00:00) < current date (2021-01-02 00:00:10) <= next execution date (2021-01-03 00:00:00)
        printing_letter = DummyOperator(
                task_id = 'printing_letter'
        )
        report_letter = DummyOperator(
                task_id = 'report_letter'
        )

        is_latest = LatestOnlyOperator(
                task_id = 'is_latest'
        )

        news_letter_user = DummyOperator(
                task_id = 'news_letter_user'
        )

        printing_letter >> report_letter >> is_latest >> news_letter_user
