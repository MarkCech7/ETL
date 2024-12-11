class MongoDB:
    def __init__(self, client, database_name, collection_name):
        self.client = client
        self.database = client[database_name]
        self.collection = self.database[collection_name]

    def insert_into_mongodb(self, data):
        result = self.collection.insert_many(data)
        return result

    def drop_collection(self):
        result = self.collection.drop()
        return result

    def group_data_by_month(self):
        pipeline = [
            {
                "$group": {
                    "_id": {"$month": "$date"}, 
                    "total_cases": {"$sum": "$cases"},
                    "total_deaths": {"$sum": "$deaths"},
                }
            }
        ]
    
        grouped_data = list(self.collection.aggregate(pipeline))
        return grouped_data

    def group_data_by_week(self):
        pipeline = [
            {
                "$group": {
                    "_id": {"$week": "$date"},
                    "total_cases": {"$sum": "$cases"},
                    "total_deaths": {"$sum": "$deaths"},
                }
            }
        ]
    
        grouped_data = list(self.collection.aggregate(pipeline))
        return grouped_data


