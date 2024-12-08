from pymongo import MongoClient

def get_collection(db, collection_name):
    client = MongoClient("localhost", 27017)
    db_name = client[db]
    collection = db_name[collection_name]
    return collection

def create_mongodb(db, collection_name):
    collection = get_collection(db, collection_name)
    return collection

def insert_into_mongodb(db, collection_name, data):
    collection = get_collection(db, collection_name)
    result = collection.insert_many(data)
    return result

def drop_collection(db, collection_name):
    collection = get_collection(db, collection_name)
    result = collection.drop()
    return result

def group_data_by_month(db, collection_name):
    collection = get_collection(db, collection_name)
    pipeline = [
        {
            "$group": {
                "_id": {"$month": "$date"}, 
                "total_cases": {"$sum": "$cases"},
                "total_deaths": {"$sum": "$deaths"},
            }
        }
    ]
    
    grouped_data = list(collection.aggregate(pipeline))
    return grouped_data

def group_data_by_week(db, collection_name):
    collection = get_collection(db, collection_name)
    pipeline = [
        {
            "$group": {
                "_id": {"$week": "$date"},
                "total_cases": {"$sum": "$cases"},
                "total_deaths": {"$sum": "$deaths"},
            }
        }
    ]
    
    grouped_data = list(collection.aggregate(pipeline))
    return grouped_data