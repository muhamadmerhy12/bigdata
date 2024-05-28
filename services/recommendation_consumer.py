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
            movie_id = int(record["movieId"])
            rating = float(record["rating"])

            # Fetch existing recommendations from Redis
            existing_recs = self.redis_client.get(f"user:{user_id}:recs")
            if existing_recs:
                recommendations = json.loads(existing_recs)
            else:
                recommendations = []

            # Append the new recommendation
            recommendations.append({"movieId": movie_id, "rating": rating})

            # Store recommendations in Redis
            self.redis_client.set(f"user:{user_id}:recs", json.dumps(recommendations))

        consumer.close()

    def start_consumer(self):
        thread = Thread(target=self.consume_recommendations)
        thread.start()
