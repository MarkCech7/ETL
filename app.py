from sql_operations import data_transform, select_from_sqlite, transform_for_mongo
from mongodb_operations import create_mongodb, insert_into_mongodb, group_data_by_month, group_data_by_week
import pandas as pd
import sqlite3
from pymongo import MongoClient
import os
import matplotlib.pyplot as plt

sqlDBPath = "C:\Projects\ExtractLoadTransform\covid_data.db"

covid_data = pd.read_csv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
    index_col=['date'], parse_dates=['date']).asfreq(freq='D')

data_for_sql = data_transform(covid_data)

if not os.path.exists(sqlDBPath):
    conn = sqlite3.connect('covid_data.db')
    data_for_sql.to_sql('covid_data', conn, if_exists='replace')
    conn.commit()
    cursor = conn.cursor()
    results = cursor.fetchall()
    conn.close()

sql_data = select_from_sqlite(dbName="covid_data.db")
data_for_mongo = transform_for_mongo(sql_data)

create_mongodb("database", "collection")
insert_into_mongodb(db="database", data=data_for_mongo, collection_name="collection")

grouped_by_month = group_data_by_month("database", "collection")
grouped_by_week = group_data_by_week("database", "collection")

df = pd.DataFrame(grouped_by_month)
if "_id" in df.columns:
    df["month"] = df["_id"]  
    df.drop(columns=["_id"], inplace=True) 

df = df.sort_values("month")

month_names = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

# Proportion of Cases by Month
plt.figure(figsize=(8, 8))
plt.pie(
    df["total_cases"],
    labels=df["month"],
    autopct="%1.1f%%",
    startangle=140,
    colors=plt.cm.Paired.colors,
)

legend_labels = [f"{num} = {name}" for num, name in month_names.items() if num in df["month"].values]
plt.legend(legend_labels, title="Months", fontsize=10, loc="upper right",  bbox_to_anchor=(0.06, 0.8))
plt.title("Proportion of Cases by Month", fontsize=16)
plt.show()

# Comparing Cases and Deaths by Month
plt.figure(figsize=(10, 6))
plt.bar(df["month"], df["total_cases"], label="Total Cases", alpha=0.7, color="blue")
plt.bar(df["month"], df["total_deaths"], label="Total Deaths", alpha=0.7, color="red", bottom=df["total_cases"])
plt.title("Monthly Total Cases and Deaths", fontsize=16)
plt.xlabel("Month", fontsize=14)
plt.ylabel("Count", fontsize=14)
plt.xticks(df["month"])
plt.legend(fontsize=12)
plt.grid(axis="y", alpha=0.5)
plt.show()