from elasticsearch import Elasticsearch, helpers
import json

# Establish connection to Elasticsearch cluster
client = Elasticsearch(
    "your-elasticsearch-cluster-url",  # Replace with your Elasticsearch cluster URL
    verify_certs=False,
    basic_auth=("your-username", "your-password")  # Replace with your Elasticsearch username and password
)

# Path to your JSON data file
json_file_path = "/path/to/yelp_academic_dataset_business.json"  # Replace with your JSON file path

# Read and parse JSON objects from file
data = []
with open(json_file_path) as f:
    for line in f:
        try:
            doc = json.loads(line.strip())
            data.append(doc)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

# Bulk upload JSON data to Elasticsearch index
helpers.bulk(client, data, index="business")
