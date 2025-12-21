from airflow import DAG                               #type:ignore
from airflow.operators.python import PythonOperator   #type:ignore
from datetime import datetime


''' task_b cannot start until task_a finishes, the 
 priority_weight determines how quickly task_a gets a worker slot compared to 
tasks in other DAGs or other branches of the same DAG. '''

def simple_task(**kwargs):
    print(kwargs)

with DAG(
    dag_id='weight_rule_demonstration',
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False
) as dag:

    # 1. DOWNSTREAM (Default)
    # This task will have a high priority because it has many downstream dependencies.
    task_a = PythonOperator(
        task_id='downstream_rule_task',
        python_callable=simple_task,
        priority_weight=10,
        weight_rule='downstream' 
    )

    # 2. ABSOLUTE
    # This task will have a weight of exactly 5, regardless of its position in the DAG.
    task_b = PythonOperator(
        task_id='absolute_rule_task',
        python_callable=simple_task,
        priority_weight=5,
        weight_rule='absolute'
    )

    # 3. UPSTREAM
    # This task's priority grows based on how many tasks came before it.
    task_c = PythonOperator(
        task_id='upstream_rule_task',
        python_callable=simple_task,
        priority_weight=1,
        weight_rule='upstream'
    )

    task_a >> task_b >> task_c