from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from transformers import pipeline
import logging
import os

# Configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Define checkpoint directory for Spark Streaming
checkpoint_dir = '/path/to/checkpoints/kafka_to_mongo'
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

# Configuration parameters for Kafka and MongoDB
config = {
    "kafka": {
        "bootstrap.servers": "your-kafka-server-url:9092",  # Replace with your Kafka server
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "your-kafka-username",             # Replace with your Kafka username
        "sasl.password": "your-kafka-password",             # Replace with your Kafka password
        "client.id": "json-serial-producer"
    },
    "mongodb": {
        "uri": "mongodb+srv://your-username:your-password@your-cluster.mongodb.net/?retryWrites=true&w=majority",  # Replace with your MongoDB URI
        "database": "reviewsDB",
        "collection": "enriched_reviews_collection"
    }
}

# Initialize the sentiment analysis pipeline using HuggingFace transformers
sentiment_pipeline = pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english")

# Function to perform sentiment analysis on text
def analyze_sentiment(text):
    if text and isinstance(text, str):
        try:
            result = sentiment_pipeline(text)[0]
            return result['label']
        except Exception as e:
            logging.error(f"Error in sentiment analysis: {e}")
            return "Error"
    return "Empty or Invalid"

# Register UDF for sentiment analysis
sentiment_udf = udf(analyze_sentiment, StringType())

# Function to read from Kafka and write enriched data to MongoDB
def read_from_kafka_and_write_to_mongo(spark):
    topic = "raw_topic"

    # Define schema for incoming Kafka JSON messages
    schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", FloatType()),
        StructField("useful", IntegerType()),
        StructField("funny", IntegerType()),
        StructField("cool", IntegerType()),
        StructField("text", StringType()),
        StructField("date", StringType())
    ])

    # Read stream data from Kafka topic
    stream_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                 .option("subscribe", topic)
                 .option("kafka.security.protocol", config['kafka']['security.protocol'])
                 .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
                 .option("kafka.sasl.jaas.config",
                         f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config["kafka"]["sasl.username"]}" '
                         f'password="{config["kafka"]["sasl.password"]}";')
                 .option("failOnDataLoss", "false")
                 .load()
                 )

    # Parse JSON messages into structured DataFrame
    parsed_df = stream_df.select(from_json(col('value').cast("string"), schema).alias("data")).select("data.*")

    # Add sentiment analysis column to DataFrame
    enriched_df = parsed_df.withColumn("sentiment", sentiment_udf(col('text')))

    # Write enriched DataFrame to MongoDB
    query = (enriched_df.writeStream
             .format("mongodb")
             .option("spark.mongodb.connection.uri", config['mongodb']['uri'])
             .option("spark.mongodb.database", config['mongodb']['database'])
             .option("spark.mongodb.collection", config['mongodb']['collection'])
             .option("checkpointLocation", checkpoint_dir)
             .outputMode("append")
             .start()
             .awaitTermination()
             )

if __name__ == "__main__":
    # Initialize Spark Session
    spark = (SparkSession.builder
             .appName("KafkaStreamToMongo")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
             .getOrCreate()
             )

    # Start streaming data from Kafka to MongoDB
    read_from_kafka_and_write_to_mongo(spark)
