from sql_db import SQLdb
from mongo_db import MongoDB
from transformations import Transformations
from visualisations import Visualisations
from pymongo import MongoClient
import pandas as pd
import sqlite3

covid_data = pd.read_csv(
    "data\covid_data.csv",
    index_col=['date'], parse_dates=['date']).asfreq(freq='D')

transforms = Transformations()
data_for_sql = transforms.data_transform(covid_data)

conn = sqlite3.connect('covid_data.db')
sql_operations = SQLdb(conn=conn)
sql_operations.insert_into_sqlite(data_for_sql)

sql_data = sql_operations.select_from_sqlite()
data_for_mongo = transforms.transform_for_mongo(sql_data)

client = MongoClient("localhost", 27017)
mongodb_operations = MongoDB(client=client, database_name="database", collection_name="collection")
mongodb_operations.insert_into_mongodb(data=data_for_mongo)

sql_grouped_by_month = sql_operations.group_data_by_month('covid_data')
sql_grouped_by_week = sql_operations.group_data_by_week('covid_data')
mongo_grouped_by_month = mongodb_operations.group_data_by_month()
mongo_grouped_by_week = mongodb_operations.group_data_by_week()

visualisations_sql = Visualisations(sql_grouped_by_month)
visualisations_sql.proportion_by_month()
visualisations_sql.compare_cases_and_deaths()

visualisations_mongo = Visualisations(mongo_grouped_by_month)
visualisations_mongo.proportion_by_month()
visualisations_mongo.compare_cases_and_deaths()
