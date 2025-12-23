from airflow import DAG                                 #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator         #type:ignore
from airflow.operators.python import PythonOperator     #type:ignore
from airflow.utils.helpers import cross_downstream      #type:ignore

def _extract_a_success(context):
        print(context)
        print(f"Luffy's dag {context['dag']}")

def _extract_b_failure(context):
        print(context)
        print(f"Exception occurred when {context['exception']}")

def _my_func(execution_date,ti):
        ti.xcom_push(key = 'store_return',value = 3)
        print("Hello world 1 print")                   
        return "Hello World 2 Return statement"   

def _pull_xcom(ti):
        x_comvalues = ti.xcom_pull(
                                task_ids = ['process_a','process_b','process_c','store'],
                                key = 'return_value'
                                )
        print(x_comvalues)


with DAG(
        dag_id = 'Xcom',
        start_date = datetime(25,12,15),
        schedule_interval = "@daily",
        catchup = True
) as dag:
        
        extract_a = BashOperator(
                owner = 'Luffy',
                task_id = 'extract_a',
                bash_command = "echo 'Task_a' && sleep 5",  # Xcom -> Task_a
                wait_for_downstream = True,
                on_success_callback = _extract_a_success,
                task_concurrency =1 
        )

        extract_b = BashOperator(
                owner = "Zoro",
                task_id = 'extract_b',
                bash_command = "echo 'Task b' && sleep 5", #Xcom -> Task_b
                on_failure_callback = _extract_b_failure,
                task_concurrency =1 
        )

        process_a = BashOperator(
                owner = "Nami",
                task_id = 'process_a',
                bash_command = "echo {{ti.priority_weight}} && sleep 5",
                pool = "process_pool",
                do_xcom_push = True            # Won't store
        )

        process_b = BashOperator(
                owner = "Ussop",
                task_id = 'process_b',
                bash_command = "echo {{ti.priority_weight}} && sleep 5",
                pool = "process_pool",
                do_xcom_push = True           #Won't Store
        )

        process_c = BashOperator(
                owner = "Sanji",
                task_id = 'process_c',
                bash_command = "echo {{ti.priority_weight}} && sleep 5",
                pool = "process_pool",
                do_xcom_push = True          #Wont store
        )

        store = PythonOperator(
                task_id = 'store',
                python_callable = _my_func
        )

        pull_xcom = PythonOperator(
                task_id = 'pull_xcom',
                python_callable = _pull_xcom
        )

        cross_downstream([extract_a,extract_b], [process_a,process_b,process_c])
        [process_a,process_b,process_c] >> store >> pull_xcom

