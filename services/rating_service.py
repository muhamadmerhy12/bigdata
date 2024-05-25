from kafka import KafkaProducer, KafkaConsumer
import json
import time
from flask import current_app as app
from threading import Thread
from services.csv_writer import CSVWriter


def initialize_kafka_producer(retries=5, delay=10):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[app.config['KAFKA_BOOTSTRAP_SERVERS']],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka Producer initialized.")
            return producer
        except Exception as e:
            print(f"Attempt {attempt + 1}/{retries}: Kafka broker not available, retrying in {delay} seconds...")
            print(f"Error: {e}")
            time.sleep(delay)
    raise Exception("Kafka broker not available after several retries.")


def initialize_kafka_consumer(retries=5, delay=10):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                app.config['KAFKA_RATINGS_TOPIC'],
                bootstrap_servers=[app.config['KAFKA_BOOTSTRAP_SERVERS']],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='rating-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Kafka Consumer initialized.")
            return consumer
        except Exception as e:
            print(f"Attempt {attempt + 1}/{retries}: Kafka broker not available, retrying in {delay} seconds...")
            print(f"Error: {e}")
            time.sleep(delay)
    raise Exception("Kafka broker not available after several retries.")


class RatingService:
    def __init__(self):
        self.producer = initialize_kafka_producer()
        self.consumer = initialize_kafka_consumer()
        self.csv_writer = CSVWriter(app.config['CSV_FILE_PATH'], ['userId', 'movieId', 'rating'])

    def send_rating(self, rating_data):
        try:
            self.producer.send(app.config['KAFKA_RATINGS_TOPIC'], value=rating_data)
            self.producer.flush()
            return {"status": "success", "message": "Rating has been sent to Kafka topic 'ratings'."}, 200
        except Exception as e:
            return {"status": "error", "message": str(e)}, 500

    def consume_ratings(self):
        for message in self.consumer:
            rating = message.value
            self.csv_writer.write_to_csv([rating])
            print(f"Consumed rating: {rating}")
        self.consumer.close()

    def start_consumer(self):
        thread = Thread(target=self.consume_ratings)
        thread.start()
