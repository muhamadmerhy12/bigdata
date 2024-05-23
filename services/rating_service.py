from kafka import KafkaProducer, KafkaConsumer
import csv
import json
import time
import atexit


def initialize_kafka_producer(retries=5, delay=10):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9093'],
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
                'ratings',
                bootstrap_servers=['localhost:9093'],
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

    def send_rating(self, rating_data):
        try:
            self.producer.send('ratings', value=rating_data)
            self.producer.flush()
            return {"status": "success", "message": "Rating has been sent to Kafka topic 'ratings'."}, 200
        except Exception as e:
            return {"status": "error", "message": str(e)}, 500

    def consume_ratings(self):
        csv_file_path = 'ratings.csv'

        with open(csv_file_path, 'a', newline='') as file:
            writer = csv.writer(file)

            # Ensure headers are written only once
            file.seek(0, 2)
            if file.tell() == 0:
                writer.writerow(['userId', 'movieId', 'rating'])
                file.flush()

            for message in self.consumer:
                rating = message.value
                writer.writerow([rating['userId'], rating['movieId'], rating['rating']])
                file.flush()  # Ensure each write is flushed to the file
                print(f"Consumed rating: {rating}")

            file.flush()

    def cleanup(self):
        self.consumer.close()


rating_service = RatingService()
atexit.register(rating_service.cleanup)


def consume_ratings():
    rating_service.consume_ratings()
