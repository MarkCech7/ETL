from operations import load_sales_data, transform_sales_data, analyze_sales_data
from covid_operations import data_transform, select_from_sqlite, transform_for_json
import pandas as pd
import sqlite3
from pymongo import MongoClient
import json
import os


sqlDBPath = "C:\Projects\ExtractLoadTransform\covid_data.db"

covid_data = pd.read_csv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
    index_col=['date'], parse_dates=['date']).asfreq(freq='D')

transformed_data = data_transform(covid_data)

if not os.path.exists(sqlDBPath):
    conn = sqlite3.connect('covid_data.db')
    transformed_data.to_sql('covid_data', conn, if_exists='replace')
    conn.commit()
    cursor = conn.cursor()
    #cursor.execute('SELECT * FROM covid_data WHERE days_since_first_case >= 500 LIMIT 5;')
    results = cursor.fetchall()
    #for row in results:
    #print(row)
    conn.close()

data = select_from_sqlite(dbName="covid_data.db")

json_compatible_data = transform_for_json(data)
#print(type(json_compatible_data))
print(json.dumps(json_compatible_data, indent=2))

client = MongoClient("localhost", 27017)
db = client["database"]
collection = db["collection"]
result = collection.insert_many(json_compatible_data)
#for doc in collection.find():
#    print(doc)

first_item = collection.find_one()
print(first_item)