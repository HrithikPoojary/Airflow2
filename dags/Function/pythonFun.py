from airflow.decorators import task #type:ignore
from airflow.operators.python import get_current_context #type:ignore
#default task_id = pythonfunction name
@task( 
        task_id = 'task_a'
)
def process(my_var):
        context = get_current_context()
        print(context)
        print(f"{my_var['path']} / {my_var['filename']}")
        return 123