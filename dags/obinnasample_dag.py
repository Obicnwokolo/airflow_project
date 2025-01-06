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

def format_responseData(responseData):
    refined_responseData = []
    for item in responseData:
        refined_responseData.append({
            'id': item['id'],
            'iso2Code': item['iso2Code'],
            'name': item['name'],
            'region': item['region']['value'],
            'adminregion': item['adminregion']['value'],
            'incomeLevel': item['incomeLevel']['value'],
            'lendingType': item['lendingType']['value'],
            'capitalCity': item['capitalCity'],
            'longitude': item['longitude'],
            'latitude': item['latitude']
        })
    return refined_responseData

def responseData_to_df(refined_responseData):
    import pandas as pd
    df = pd.DataFrame(refined_responseData)

    print("Preview of the data to be written:")
    print(df.head())

    return df


def write_to_db(df):
    import sqlalchemy as sa
    from sqlalchemy import create_engine, inspect, text
    from urllib.parse import quote_plus
    from sqlalchemy import Table, MetaData
    
    # Creating connection strings for my database
    username= "consultants"
    password = quote_plus("WelcomeItc@2022")
    host= "18.132.73.146"
    port = "5432"
    database= "testdb"
    
    # Create database connection string
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    # Write DataFrame to database
    try:
        df.to_sql('worldbank_data', con=engine, if_exists='replace', index=False)
        print("Data successfully added to the database.")
    except Exception as e:
        print(f"Failed to write data to the database: {e}")

def stream_data():
    import json
    from kafka import kafkaProducer
    import time
    import logging

    producer = kafkaProducer(bootstrap_servers=['ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092'], max_block_ms=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time +60:
            break
        try:
            res = get_data()
            res = format_responseData(res)

            producer.send('countries_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.erro(f"An error occured: {e}")
            continue


dag = DAG(
    'obinnasample_dag',
    default_args=default_args,
    description= 'world countires etl code',
    schedule_interval = '@daily',
    catchup= False
)
    
run_get_data =PythonOperator(
    task_id = 'get_data',
    python_callable=get_data,
    provide_context=True,
    dag=dag,
)

run_format_responseData =PythonOperator(
    task_id = 'format_responseData',
    python_callable=format_responseData,
    dag=dag,
)

run_responseData_to_df =PythonOperator(
    task_id = 'responseData_to_df',
    python_callable=responseData_to_df,
    dag=dag,
)

run_write_to_db =PythonOperator(
    task_id = 'write_to_db',
    python_callable=write_to_db,
    provide_context=True,
    dag=dag,
)

run_stream_data =PythonOperator(
    task_id = 'data_stream',
    python_callable=stream_data,
    provide_context=True,
    dag=dag,
)

run_get_data >> run_format_responseData >> run_responseData_to_df >> [run_write_to_db, run_stream_data]