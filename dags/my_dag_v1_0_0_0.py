from airflow import DAG #type:ignore
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator #type:ignore

# Dag level
defualt_args = {
        "email" : ['luffy@gamil'],    # we can use different mails here,  By default we will recieve failed and retry tsk mail
        "email_on_retry" : False,     # we won't get mail on retry
        "email_on_failure" : False,   # we won't get mail on failure
}

# Airflow Level
# Airflow.cfg  
# default_email_on_retry = True
# default_email_on_failure = True


with DAG(
        dag_id = 'my_dag_v1_0_0_0',
        start_date = datetime(21,1,1),
        defualt_args = defualt_args,
        schedule_interval = '@daily',
        catchup = False
         ) as dag :
        
        # airflow.cfg
        #  ****security.google.com/settings/security/apppasswords  --Generate password
        # smtp_host = smtp.gmail.com
        # smtp_starttls = True
        # smtp_ssl = False
        # Example: smtp_user = airflow
        # smtp_user = luffy*******@gmail
        # Example: smtp_password = airflow
        # smtp_password =  *********zoro***
        # smtp_port = 587
        # smtp_mail_from = luffy*******@gmail
        # smtp_timeout = 30
        # smtp_retry_limit = 5

        # Whether email alerts should be sent when a task is retried
        #      default_email_on_retry = True

        # Whether email alerts should be sent when a task failed
        #      default_email_on_failure = True


        
        task_a = BashOperator(
                owner = 'Macr',
                task_id = "taks_a",
                bash_command = "echo 'Task A'"
        )
        #Task level 
        task_b = BashOperator(
                owner = 'Luffy',
                task_id = "task_b",
                retries = 3,
                retry_delay = timedelta(seconds = 10),
                retry_exponential_backoff = True,
                bash_command = "echo '{{ti.try_number}}' && exit 1"
        )

        # > docker compose down && docker-compose up -d
        # > docker ps  
        # > docker exec -t <airflow-scheduler> /bin/bash
        # > airflow tasks test <dag_id> <task_id> <execution_dt(logic_dt)>

        task_a >> task_b