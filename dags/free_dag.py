from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

default_args = {
    'owner': 'obinna',
    'depends_on_past': False,
    'start_date' : datetime(2025, 1, 3),
    'retries': 1,
    'retry_delay' : timedelta(minutes=1)
    }

def get_data():
    import requests
    
    url = 'https://api.worldbank.org/v2/country/br?format=json'
    header ={"Content-Type":"application/json",
            "Accept-Encoding":"deflate"}
    response = requests.get(url,headers=header)
    print(response)
    responseData = response.json()
    responseData = responseData[1]
    
    return responseData

def hello_sujay():
    print("hello Sujay")

def hello_uttam():
    print("hello Uttam")

def hello_hitesh():
    print("hello hitesh")

def hello_prashob():
    print("hello prashob")

def hello_hasnain():
    print("hello hasnain")

def hello_mucteba():
    print("hello mucteba")

def hello_obinna():
    print("hello obnna")

dag = DAG(
    'obinna_free_dag',
    default_args=default_args,
    description= 'hello BD team',
    schedule_interval = '@daily',
    catchup= False
)
    
run_get_data =PythonOperator(
    task_id = 'get_data',
    python_callable=get_data,
    provide_context=True,
    dag=dag,
)

run_hello_sujay =PythonOperator(
    task_id = 'hello_sujay',
    python_callable=hello_sujay,
    dag=dag,
)

run_hello_uttam =PythonOperator(
    task_id = 'hello_uttam',
    python_callable=hello_uttam,
    dag=dag,
)

run_hello_hitesh =PythonOperator(
    task_id = 'hello_hitesh',
    python_callable=hello_hitesh,
    dag=dag,
)

run_hello_prashob =PythonOperator(
    task_id = 'hello_prashob',
    python_callable=hello_prashob,
    dag=dag,
)

run_hello_hasnain =PythonOperator(
    task_id = 'hello_hasnain',
    python_callable=hello_hasnain,
    dag=dag,
)

run_hello_mucteba =PythonOperator(
    task_id = 'hello_mucteba',
    python_callable=hello_mucteba,
    dag=dag,
)

run_hello_obinna =PythonOperator(
    task_id = 'hello_obinna',
    python_callable=hello_obinna,
    dag=dag,
)

run_get_data >> [run_hello_prashob, run_hello_sujay] >> run_hello_uttam >> [run_hello_mucteba, run_hello_hasnain, run_hello_hitesh] >> run_hello_obinna