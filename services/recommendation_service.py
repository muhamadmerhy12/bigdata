from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode
import redis
import json


class RecommendationService:
    def __init__(self):
        self.spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)

    def get_recommendations(self, user_id):
        # Check cache first
        cached_recs = self.redis_client.get(f"user:{user_id}:recs")
        if cached_recs:
            return json.loads(cached_recs)

        # Load Ratings Data from CSV
        ratings = self.spark.read.csv("ratings.csv", header=True)
        ratings = ratings.withColumn("userId", ratings["userId"].cast(IntegerType())) \
            .withColumn("movieId", ratings["movieId"].cast(IntegerType())) \
            .withColumn("rating", ratings["rating"].cast(FloatType()))

        # Train ALS Model
        als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
        model = als.fit(ratings)

        # Generate Recommendations for All Users
        user_recommendations = model.recommendForAllUsers(10)
        user_recommendations = user_recommendations.withColumn("recommendation", explode(col("recommendations")))
        user_recommendations = user_recommendations.select("userId", col("recommendation.movieId"),
                                                           col("recommendation.rating"))

        print(f"{user_id}: {user_recommendations.show()}")

        # Fetch recommendations for the specified user
        recommendations = user_recommendations.filter(user_recommendations["userId"] == user_id).collect()
        recommendations_list = [{"movieId": rec["movieId"], "rating": rec["rating"]} for rec in recommendations]

        # Cache the recommendations in Redis
        self.redis_client.set(f"user:{user_id}:recs", json.dumps(recommendations_list))

        return recommendations_list


recommendation_service = RecommendationService()
