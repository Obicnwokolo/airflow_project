from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator



def print_hello():
    print('=====================================================Hello world from first Airflow DAG!')

dag1 = DAG('hello_world2', description='Hello World DAG2',start_date=days_ago(1),
          catchup=False)

hiveProcess= PythonOperator(task_id='hiveprocess', python_callable=print_hello, dag=dag1)

db = DummyOperator(task_id='DB1', retries = 3, dag=dag1)

email= DummyOperator(task_id='email', retries = 3, dag=dag1)

logfile= DummyOperator(task_id='logfile', retries = 3, dag=dag1)


sparkProcess = BashOperator(
    task_id='linuxcommand', bash_command='echo 1', dag=dag1)



db >> [hiveProcess,sparkProcess] >> email


logfile >> [hiveProcess,sparkProcess] >> email
