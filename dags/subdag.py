from airflow.models import DAG #type:ignore
from airflow.operators.bash import BashOperator #type:ignore


def subdag_factory(parent_dag_id,subdag_id,default_args):
        with DAG(
                dag_id = f"{parent_dag_id}.{subdag_id}",
                default_args = default_args
        ) as dag:
                
                training_a = BashOperator(task_id = 'training_a',bash_command = "echo 'training_a' ")
                training_b = BashOperator(task_id = 'training_b',bash_command = "echo 'training_b' ")
                training_c = BashOperator(task_id = 'training_c',bash_command = "echo 'training_c' ")

        return dag