from datetime import datetime
from kafka import KafkaProducer
import json
import random
import time
import os
import yaml

# Load configuration from YAML file
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

class Event:
    def __init__(self, reporter_id, metric_id, metric_value, message):
        self.reporter_id = reporter_id
        self.timestamp = datetime.now()
        self.metric_id = metric_id
        self.metric_value = metric_value
        self.message = message

    def to_dict(self):
        return{
            'reporterId': self.reporter_id,
            'timestamp': self.timestamp.isoformat(),
            'metricId': self.metric_id,
            'metricValue': self.metric_value,
            'message': self.message
        }    
        

def read_last_reporter_id():
    try:
        with open('last_reporter_id.txt', 'r') as file:
            last_reporter_id = int(file.read().strip())
    except FileNotFoundError:
        last_reporter_id = config["last_reporter_id_start"]
    return last_reporter_id


def write_last_reporter_id(reporter_id):
    with open('last_reporter_id.txt', 'w') as file:
        file.write(str(reporter_id))


producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"), value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def produce_events():
    reporter_id_counter = read_last_reporter_id() + config["counter_increase"]

    while True:
        event = Event(
            reporter_id=reporter_id_counter,
            metric_id=random.randint(config['random_range']['min'], config['random_range']['max']),
            metric_value=random.randint(config['random_range']['min'], config['random_range']['max']),
            message="Hello World"
        )
        producer.send("events", event.to_dict())
        print(f"Produced event: {event.to_dict()}")

        write_last_reporter_id(reporter_id_counter)
        reporter_id_counter += config["counter_increase"]
        
        time.sleep(config["sleep_time_producer"])


if __name__ == "__main__":
    produce_events()       