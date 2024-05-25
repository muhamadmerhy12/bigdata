from kafka import KafkaConsumer
import redis
import json
from threading import Thread


class RecommendationConsumerService:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)

    def consume_recommendations(self):
        consumer = KafkaConsumer(
                'recommendations',
                bootstrap_servers=['localhost:9093'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='recommendations-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

        for message in consumer:
            record = message.value
            user_id = record['userId']
            recommendations = record['recommendations']

            # Convert movieId and rating to correct types if necessary
            recommendations = [{"movieId": int(rec["movieId"]), "rating": float(rec["rating"])} for rec in
                               recommendations]

            # Store recommendations in Redis
            self.redis_client.set(f"user:{user_id}:recs", json.dumps(recommendations))

        consumer.close()

    def start_consumer(self):
        thread = Thread(target=self.consume_recommendations)
        thread.start()
