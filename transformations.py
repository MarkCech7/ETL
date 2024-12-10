import pandas as pd

class Transformations:
    def data_transform(self, data):
        data['cases'] = data['cases'].astype(int)
        data['deaths'] = data['deaths'].astype(int)

        #difference
        data['new_cases'] = data['cases'].diff()

        #moving average
        data['new_cases_ma7'] = data['new_cases'].rolling(7).mean().round(3)
    
        #fatality rate
        data['case_fatality_rate'] = (data['deaths'] / data['cases']) * 100
        data['case_fatality_rate'] = data['case_fatality_rate'].fillna(0).round(3)

        #rolling average
        data['case_rolling_avg'] = data['cases'].rolling(7).mean().round(3)
        data['deaths_rolling_avg'] = data['deaths'].rolling(7).mean().round(3)

        #days_since_first_case
        first_case_date = data['cases'].gt(0).idxmax()
        data['days_since_first_case'] = (data.index - first_case_date).days

        return data
    
    def transform_for_mongo(self, data):
        data['date'] = pd.to_datetime(data['date'])    
        data.columns = [
            col.lower().replace(' ', '_')
            for col in data.columns
        ]

        transformed_data = data.to_dict(orient='records')
        return transformed_data
