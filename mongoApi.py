import os
from pymongo import MongoClient, errors
import json
from bson import ObjectId
from datetime import datetime

def connect_to_cosmos_db(connection_string):
    """Establish connection to Azure Cosmos DB using MongoDB API."""
    try:
        client = MongoClient(connection_string)
        # Check server connection
        client.server_info()  # Forces a call to check if the server is reachable
        print("Connection to Cosmos DB successful.")
        return client
    except errors.ServerSelectionTimeoutError as e:
        print(f"ServerSelectionTimeoutError: {e}")
        return None
    except errors.ConnectionFailure as e:
        print(f"ConnectionError: {e}")
        return None

def read_many(client, database_name, collection_name, filter_condition):
    """Retrieve multiple documents from Cosmos DB and store them in a list."""
    data_list = []
    try:
        db = client[database_name]
        collection = db[collection_name]
        items = collection.find(filter_condition)

        for item in items:
            data_list.append(item)

        print(f"Retrieved {len(data_list)} documents.")
        return data_list
    except Exception as e:
        print(f"An error occurred while reading data: {e}")
        return []
    
def json_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()  # Converts datetime to ISO 8601 string format
    elif isinstance(obj, dict):
        return {k: json_serializer(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [json_serializer(i) for i in obj]
    raise TypeError(f"Type {type(obj)} not serializable")


def main(filterID, facilityID):
    # Configuration
    connection_string = os.environ.get('COSMOS_CONNECTION_STRING')
    database_name = os.environ.get('DB_NAME')
    collection_name = os.environ.get('COLLECTION_NAME')
    filter_condition = {'filterid': 106416, 'type': 'reports'}

    # Connect to Cosmos DB
    client = connect_to_cosmos_db(connection_string)
    if client:
        # Read data
        data = read_many(client, database_name, collection_name, filter_condition)
        # Process data as needed
        if data:
            transformed_data = []
            for item in data:
                folderName = item["noteinfo"]["folder_name"]
                filename = item['noteorfilename']
                filepath = f"{os.environ.get('FILEPATH_URL')}{facilityID}/sanityscripts/{folderName}/{filename}"

                transformed_item = {
                    "_id": item["_id"],
                    "facilityId": facilityID,  # Use facilityID from function argument
                    "type": item["type"],
                    "filename": filename,  # Rename 'noteorfilename' to 'filename'
                    "filePath": filepath,  # Construct file path
                    "author": item["author"],
                    "time": item["time"],
                    "reportState": item.get("reportstate"),  # Rename 'reportstate' to 'reportState'
                    "createdAt": item.get("createdAt"),  # Ensure 'createdAt' format is correct
                    "updatedAt": item.get("updatedAt"),  # Ensure 'updatedAt' format is correct
                    "__v": item.get("__v")
                }

                transformed_data.append(transformed_item)

            output_file = f'{filterID}.json'
            with open(output_file, 'w') as f:
                json.dump(transformed_data, f, indent=4, default=json_serializer)  # Indent for readability
            print(f"Data has been written to {output_file}")
        else:
            print("No data retrieved or failed to retrieve data.")
    else:
        print("Failed to connect to Cosmos DB.")

# if __name__ == '__main__':
#     filterId = '106416'
#     facilityId = 'efd503e8-16a7-4f12-8f10-0caf93853cba'
#     main(filterId,facilityId)

