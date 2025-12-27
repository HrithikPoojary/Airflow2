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
                
                with TaskGroup('task_group_publish_id') as publish_tasks:
                        publish_a = BashOperator(task_id = 'publish_a',
                                                bash_command = "echo 'publish_a'")
                        
                        publish_b = BashOperator(task_id = 'publish_b',
                                                bash_command = "echo 'publish_b'")
                        
                        publish_c = BashOperator(task_id = 'publish_c',
                                                bash_command = "echo 'publish_c'")
        return training_tasks
