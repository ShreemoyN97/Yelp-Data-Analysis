from elasticsearch import Elasticsearch, helpers
import json

# connect to elasticsearch cloud
client = Elasticsearch(
    "<elasticsearch_cluster_url>",
    verify_certs=False,
    basic_auth=("<user_name>", "<password>")
)

# Open the file and read json object
with open("/kaggle/input/yelp-dataset/yelp_academic_dataset_business.json") as f:
    data = []
    for line in f:
        try:
            doc = json.loads(line.strip())
            data.append(doc)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            
    helpers.bulk(client, data, index="business")
