import os
import tempfile
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode
import redis
import json
from pymongo import MongoClient
from kafka import KafkaProducer
from flask import current_app
from services.movie_service import MovieService
from flask import current_app as app


def cleanup(temp_ratings_path, temp_dir):
    if os.path.exists(temp_ratings_path):
        os.remove(temp_ratings_path)
    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)


class RecommendationService:
    def __init__(self):
        self.hdfs_client = InsecureClient(app.config['HDFS_URL'], user='hdfs')
        self.directory_path = app.config['HDFS_DIRECTORY_PATH']
        self.hdfs_file_path = f"""{self.directory_path}{app.config['HDFS_CSV_PATH']}"""

        self.spark = (SparkSession.builder
                      .appName("MovieRecommendation")
                      .getOrCreate())

        self.redis_client = redis.Redis(
            host=current_app.config['REDIS_HOST'],
            port=current_app.config['REDIS_PORT'],
            db=current_app.config['REDIS_DB']
        )

        self.movie_service = MovieService()

        self.kafka_producer = KafkaProducer(
            bootstrap_servers=current_app.config['KAFKA_BOOTSTRAP_SERVERS'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def download_ratings_file(self, local_path):
        if not os.path.exists(local_path):
            self.hdfs_client.download(self.hdfs_file_path, local_path)

    def generate_recommendations_for_all_users(self, num_recommendations=10):
        temp_dir = tempfile.mkdtemp()
        temp_ratings_path = os.path.join(temp_dir, app.config['RATINGS_CSV_PATH'])
        self.download_ratings_file(temp_ratings_path)
        ratings = self.spark.read.csv(temp_ratings_path, header=True)

        # Cast Columns to Correct Types
        ratings = ratings.withColumn("userId", ratings["userId"].cast(IntegerType())) \
            .withColumn("movieId", ratings["movieId"].cast(IntegerType())) \
            .withColumn("rating", ratings["rating"].cast(FloatType()))

        # Train ALS Model
        als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
        model = als.fit(ratings)

        # Generate Recommendations for all users
        user_recommendations = model.recommendForAllUsers(num_recommendations)

        # Explode the recommendations column to view the results better
        user_recommendations = user_recommendations.withColumn("recommendation", explode(col("recommendations")))

        # Select and display userId, movieId, and rating from recommendations
        user_recommendations = user_recommendations.select("userId", col("recommendation.movieId"),
                                                           col("recommendation.rating"))

        recommendations = user_recommendations.collect()
        recommendations_collection = MongoClient(current_app.config['MONGO_URI'])['movie_db']['recommendations']

        for row in recommendations:
            user_id = row["userId"]
            movie_id = row["movieId"]
            rating = row["rating"]

            self.kafka_producer.send(current_app.config['KAFKA_RECOMMENDATIONS_TOPIC'],
                                     {'userId': user_id, 'movieId': movie_id, 'rating': rating})
            recommendations_collection.insert_one({
                "userId": user_id,
                "movieId": movie_id,
                "rating": rating
            })

        cleanup(temp_ratings_path, temp_dir)
        self.spark.stop()

    def get_recommendations_by_user_id(self, user_id):
        # Check cache first
        cached_recommendations = self.redis_client.get(f"user:{user_id}:recs")
        if not cached_recommendations:
            # If not found in Redis, generate recommendations for all users
            self.generate_recommendations_for_all_users()

            # Fetch again from Redis after generating recommendations
            cached_recommendations = self.redis_client.get(f"user:{user_id}:recs")

        recommendations = json.loads(cached_recommendations)

        # Get movie details for the recommendations
        movie_ids = [rec["movieId"] for rec in recommendations]
        movies = list(self.movie_service.collection.find({"movieId": {"$in": movie_ids}}, {'_id': 0}))

        # Combine recommendations with movie details
        detailed_recommendations = []
        for rec in recommendations:
            for movie in movies:
                if movie["movieId"] == rec["movieId"]:
                    detailed_recommendations.append({**movie, "rating": rec["rating"]})
                    break

        return detailed_recommendations
