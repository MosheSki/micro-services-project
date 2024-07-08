import pymongo
import redis
import json
import time
from datetime import datetime
import threading
import os
import yaml

# Load configuration from YAML file
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

uri = os.getenv("MONGO_DB_URI")

# MongoDB connection setup
mongo_client = pymongo.MongoClient(uri)
mongo_db = mongo_client["event_db"]
mongo_collection = mongo_db["events"]

# Redis connection setup
redis_client = redis.StrictRedis(host='redis', port=config["redis_port"], db=config["redis_db"])
   

# Function to convert MongoDB document to JSON serializable format
def convert_doc(doc):
    doc['_id'] = str(doc['_id'])
    doc['timestamp'] = doc['timestamp'].isoformat()  # convert datetime to string
    return doc

# Function to transfer data from MongoDB to Redis
def transfer_data():
    # Retrieve the last timestamp from Redis
    last_timestamp = redis_client.get('last_timestamp')
    if last_timestamp:
        last_timestamp = datetime.strptime(last_timestamp.decode('utf-8'), "%Y-%m-%dT%H:%M:%S.%f")
    else:
        last_timestamp = datetime.min

    # Query MongoDB for documents with a timestamp greater than the last one copied
    new_documents = mongo_collection.find({"timestamp": {"$gt": last_timestamp}}).sort("timestamp")

    for doc in new_documents:
        doc = convert_doc(doc)  # Convert the document to a JSON serializable format
        reporter_id = doc['reporterId']
        timestamp = doc['timestamp']
        timestamp_str = timestamp.replace(":", "").replace("-", "").replace("T", "")
        key = f"{reporter_id}:{timestamp_str}"
        value = json.dumps(doc)
        
        # Set the data in Redis
        redis_client.set(key, value)
        
        # Update the last copied timestamp
        redis_client.set('last_timestamp', timestamp)

        print(f"Transferred document with key: {key}")

# Function to run the transfer_data function every 30 seconds
def run_periodically():
    while True:
        transfer_data()
        time.sleep(config["sleep_time_redis"])

# Start the periodic function in a separate thread
thread = threading.Thread(target=run_periodically)
thread.start()