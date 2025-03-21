import json
import os
import pandas as pd
from confluent_kafka import Producer

# Kafka producer configuration with placeholders
conf = {
    "bootstrap.servers": "your-bootstrap-server-url:9092",  # Replace with your Kafka bootstrap server
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "your-kafka-username",                  # Replace with your Kafka username
    "sasl.password": "your-kafka-password",                  # Replace with your Kafka password
    "client.id": "json-serial-producer"
}

# Create Kafka Producer instance
producer = Producer(conf)

# Define Kafka topic
topic = "raw_topic"  # Replace with your actual Kafka topic name

# Delivery report callback function
def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered successfully! Key: {msg.key()}")

# Read checkpoint from file
def read_checkpoint(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as file:
            return int(file.read().strip())
    return 0

# Write checkpoint to file
def write_checkpoint(checkpoint_file, index):
    with open(checkpoint_file, 'w') as file:
        file.write(str(index))
    print(f"Checkpoint updated to line: {index}")

# Handle date formatting for JSON serialization
def handle_date(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# Stream JSON data to Kafka serially
def stream_json_serially(file_path, checkpoint_file='/path/to/checkpoint.txt'):
    last_sent_index = read_checkpoint(checkpoint_file)

    with open(file_path, 'r') as file:
        for idx, line in enumerate(file):
            if idx < last_sent_index:
                continue

            try:
                record = json.loads(line)
                producer.produce(
                    topic,
                    key=str(record['review_id']),
                    value=json.dumps(record, default=handle_date).encode('utf-8'),
                    callback=delivery_report
                )

                producer.flush()
                write_checkpoint(checkpoint_file, idx + 1)

            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")

if __name__ == "__main__":
    # Path to your input JSON data file
    stream_json_serially('/path/to/yelp_academic_dataset_review.json')  # Replace with your data file path
