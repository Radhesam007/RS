from datetime  import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import bashoperator

default_arg ={
    'owner': 'airflow',
    'depends_on_failure' : datetime(2022,1,1),
    'email-on_failure' : False,
    'email_on retry' : False,
    'retries' : 1, 
}

dag = DAG(
    'example_dag',
    default_arg = default_arg,
    description = 'an example DAG',
    schadule_interval=timedelta(days=1)   
)
t1 = bashoperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)
t2 = bashoperator(
    task_id='sleep',
    bash_command="sleep5",
    retrives=3,
    dag=dag,
)
t3 = bashoperator(
    task_id='print_hello',
    bash_command = 'echo"hello_world"',
    dag=DAG
)

t1 >> t2 >>t3