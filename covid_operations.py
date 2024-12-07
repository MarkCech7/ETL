import sqlite3
import pandas as pd
"""
Add a case_fatality_rate feature: deaths / cases * 100.
Compute rolling averages for smoother trends.
Derive days_since_first_case as a time-based feature.
"""

def data_transform(data):

    #konverzia na datove typy
    data['cases'] = data['cases'].astype(int)
    data['deaths'] = data['deaths'].astype(int)

    #*** transformacie ***

    #diff - rozdiel medzi dvoma po sebe nasledujucimi riadkami
    data['new_cases'] = data['cases'].diff()

    #moving average = kĺzavý priemer -> zoberie hodnoty daného dňa a predchádzajúcich 6 dní, a následne vypočíta ich priemer
    data['new_cases_ma7'] = data['new_cases'].rolling(7).mean().round(3)

    # 1. fatality rate
    data['case_fatality_rate'] = (data['deaths'] / data['cases']) * 100
    #osetrenie delenia nulou alebo naN
    data['case_fatality_rate'] = data['case_fatality_rate'].fillna(0).round(3)

    # 2. klzavy priemer
    data['case_rolling_avg'] = data['cases'].rolling(7).mean().round(3)
    data['deaths_rolling_avg'] = data['deaths'].rolling(7).mean().round(3)

    # 3. days_since_first_case
    first_case_date = data['cases'].gt(0).idxmax()
    data['days_since_first_case'] = (data.index - first_case_date).days

    return data

def select_from_sqlite(dbName):

    conn = sqlite3.connect(dbName)

    query = "SELECT * FROM covid_data"
    data = pd.read_sql_query(query, conn)

    conn.close()

    return data

def transform_for_json(data):
    data.columns = [
        col.lower().replace(' ', '_')
        for col in data.columns
    ]
    
    """
    transformed_data = data.groupby('date').apply(
        lambda group: group.to_dict(orient='records')
    ).to_dict()
    """

    transformed_data = data.to_dict(orient='records')

    return transformed_data
