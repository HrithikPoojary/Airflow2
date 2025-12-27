from airflow.utils.task_group import TaskGroup #type:ignore
from airflow.operators.bash import BashOperator #type:ignore

def task_group():
        with TaskGroup(# taskgroup_id = 
                       'task_group_id') as training_tasks:
                training_a = BashOperator(task_id = 'training_a',
                                        bash_command = "echo 'training_a'")
                
                training_b = BashOperator(task_id = 'training_b',
                                        bash_command = "echo 'training_b'")
                
                training_c = BashOperator(task_id = 'training_c',
                                        bash_command = "echo 'training_c'")
        return training_tasks
