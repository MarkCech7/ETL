import pandas as pd

class SQLdb:
    def __init__(self, conn):
        self.conn = conn

    def select_from_sqlite(self):
        query = "SELECT * FROM covid_data"
        data = pd.read_sql_query(query, self.conn)
        return data
    
    def insert_into_sqlite(self, data):
        data.to_sql('covid_data', self.conn, if_exists='replace')
        self.conn.commit()

    def group_data_by_month(self, table_name):
        query = f"""
        SELECT 
            strftime('%m', date) AS month, 
            SUM(cases) AS total_cases, 
            SUM(deaths) AS total_deaths
        FROM 
            {table_name}
        GROUP BY 
            strftime('%m', date);
        """
        result = pd.read_sql_query(query, self.conn)
        return result

    def group_data_by_week(self, table_name):
        query = f"""
        SELECT 
            strftime('%W', date) AS week, 
            SUM(cases) AS total_cases, 
            SUM(deaths) AS total_deaths
        FROM 
            {table_name}
        GROUP BY 
            strftime('%W', date);
        """
        result = pd.read_sql_query(query, self.conn)
        return result