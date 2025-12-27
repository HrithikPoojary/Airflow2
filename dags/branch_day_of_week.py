from airflow.models import DAG #type:ignore
from datetime import datetime
from airflow.operators.dummy import DummyOperator #type:ignore
from airflow.operators.weekday import BranchDayOfWeekOperator #type:ignore
from airflow.utils.weekday import WeekDay #type:ignore

with DAG(
        dag_id = 'day_of_week_dag',
        start_date = datetime(25,12,25),
        schedule_interval = '@daily',
        catchup = False
) as dag:
        
        task_a = DummyOperator(
                task_id = 'task_a'
        )

        task_b = DummyOperator(
                task_id = 'task_b'
        )

        is_wednesday = BranchDayOfWeekOperator(
                task_id = 'is_wednesday',
                follow_task_ids_if_true = ['task_c'],
                follow_task_ids_if_false = ['end'],
                week_day = WeekDay.SATURDAY,
                # This will execution day, we have to execution date for back dated and proper run info
                # default it will current date 
                use_task_execution_day = True
        )

        task_c = DummyOperator(
                task_id = 'task_c'
        )

        end = DummyOperator(
                task_id='end'
        )

        task_a >> task_b >> is_wednesday >> [task_c,end]