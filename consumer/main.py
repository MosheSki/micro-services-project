from kafka import KafkaConsumer
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import os
import yaml

# Load configuration from YAML file
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

uri = os.getenv("MONGO_DB_URI")


client = MongoClient(uri, server_api=ServerApi('1'))
db = client['event_db']  
events_collection = db['events']

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# consumer = KafkaConsumer('events', bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"), value_deserializer=lambda v: json.loads(v.decode('utf-8')))
consumer = KafkaConsumer(
    'events',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading from the earliest offset
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='my-consumer-group'  # Consumer group ID
)


# def consume_events():
#     for message in consumer:
#         event = message.value
#         print(f"Consumed event: {event}")

# if __name__ == "__main__":
#     consume_events()

try:
   for message in consumer:
    event = message.value
    event['timestamp'] = datetime.strptime(event['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')  # Example format adjustment
    events_collection.insert_one(event)
    print(f"Inserted event: {event}")
except Exception as e:
    print(f"Error consuming messages: {e}")
finally:
    client.close()