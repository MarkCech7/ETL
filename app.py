from operations import load_sales_data, transform_sales_data, analyze_sales_data
from covid_operations import data_transform
import pandas as pd
import sqlite3

covid_data = pd.read_csv(
    'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv',
    index_col=['date'], parse_dates=['date']).asfreq(freq='D')

transformed_data = data_transform(covid_data)

conn = sqlite3.connect('covid_data.db')
transformed_data.to_sql('covid_data', conn, if_exists='replace')
conn.commit()
cursor = conn.cursor()
cursor.execute('SELECT * FROM covid_data WHERE days_since_first_case >= 500 LIMIT 5;')
results = cursor.fetchall()
for row in results:
    print(row)
conn.close()