from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, StructType, StructField
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode
import redis
import json
from pymongo import MongoClient
from kafka import KafkaProducer
from flask import current_app
from pyspark.sql import Row
from services.movie_service import MovieService


class RecommendationService:
    def __init__(self):
        self.spark = (SparkSession.builder
                      .appName("MovieRecommendation")
                      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
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

        # Cache movies data
        movies = self.movie_service.get_all_movies()
        self.movies_df = self.spark.createDataFrame([Row(**movie) for movie in movies])
        self.movies_df.cache()

    def get_recommendations(self, user_id):
        # Check cache first
        cached_recommendations = self.redis_client.get(f"user:{user_id}:recs")
        if cached_recommendations:
            return json.loads(cached_recommendations)

        # Load Ratings Data from CSV
        ratings_schema = StructType([
            StructField("userId", IntegerType(), True),
            StructField("movieId", IntegerType(), True),
            StructField("rating", FloatType(), True)
        ])
        ratings_df = self.spark.read.csv("ratings.csv", schema=ratings_schema, header=True)
        ratings_df.cache()

        # Combine Ratings and Movies Data
        ratings_movies = ratings_df.join(self.movies_df, on="movieId", how="inner")

        # Train ALS Model
        als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
        model = als.fit(ratings_df)

        # Generate Recommendations for All Users
        user_recommendations = model.recommendForAllUsers(10)
        user_recommendations = user_recommendations.withColumn("recommendation", explode(col("recommendations")))
        user_recommendations = user_recommendations.select("userId", col("recommendation.movieId"),
                                                           col("recommendation.rating"))

        # Fetch recommendations for the specified user
        recommendations = user_recommendations.filter(user_recommendations["userId"] == user_id).collect()
        recommendations_list = [{"movieId": int(rec["movieId"]), "rating": float(rec["rating"])} for rec in
                                recommendations]

        # Publish recommendations to Kafka
        self.kafka_producer.send(current_app.config['KAFKA_RECOMMENDATIONS_TOPIC'],
                                 {'userId': user_id, 'recommendations': recommendations_list})

        # Store recommendations in MongoDB
        recommendations_collection = MongoClient(current_app.config['MONGO_URI'])['movie_db']['recommendations']
        recommendations_collection.update_one(
            {"userId": user_id},
            {"$set": {"recommendations": recommendations_list}},
            upsert=True
        )

        return recommendations_list
