import pandas as pd
import json
from datetime import datetime
import requests

def run_worldbank_etl():

    import sqlalchemy as sa

    # importing connection engine pack
    from sqlalchemy import create_engine, inspect, text
    from urllib.parse import quote_plus #why
    from sqlalchemy import Table, MetaData

    # # Creating connection strings for my database
    # username= "consultants"
    # password = "WelcomeItc@2022"
    # host= "18.132.73.146"
    # port = "5432"
    # database= "testdb"
    # #ENCODED_PASSWORD = quote_plus(password)


    # #creating database connectionw string
    # connection_string = engine = create_engine('postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb')
    # # Establishing connection with engine & database
    # engine = create_engine(connection_string)
    # print("Connection Successful")


    url = 'https://api.worldbank.org/v2/country/br?format=json'
    header ={"Content-Type":"application/json",
            "Accept-Encoding":"deflate"}
    response = requests.get(url,headers=header)
    print(response)
    responseData = response.json()
    #print(responseData[1])

    selected_responseData = responseData[1]

    refined_responseData = []
    for item in selected_responseData:
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

    # Creating the DataFrame
    df = pd.DataFrame(refined_responseData)
    print(df.head())

    # df.to_sql('sworldbank_data',con=engine, if_exists= 'replace', index= False)
    # print("Data successfully added to database")
