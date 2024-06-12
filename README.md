# Recommendation System API

## Overview

This project is a Flask-based web application that provides recommendation services. It handles recommendations and ratings by utilizing several microservices and integrating with Kafka, HDFS, Redis, MongoDB, and Apache Spark.

## Table of Contents
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
- [Services](#services)
  - [CSVWriter](#csvwriter)
  - [DockerService](#dockerservice)
  - [MovieService](#movieservice)
  - [RatingService](#ratingservice)
  - [RecommendationConsumerService](#recommendationconsumerservice)
  - [RecommendationService](#recommendationservice)

## Running the Application

1. **Start the Flask application:**
    ```sh
    python app.py
    ```

2. The application will be running at `http://127.0.0.1:5000/`.

## API Endpoints

### Get Recommendations by User ID

- **URL:** `/recommendations/<int:user_id>`
- **Method:** `GET`
- **Description:** Fetches recommendations for a specific user.
- **Response:**
    - **Content-Type:** `application/json`
    - **Body:** JSON array of recommendations, including movie details and ratings.

### Send Rating

- **URL:** `/send_rating`
- **Method:** `POST`
- **Description:** Sends a rating to the rating service.
- **Request Body:**
    - **Content-Type:** `application/json`
    - **Body:** JSON object containing rating data. Example:
      ```json
      {
        "userId": 1,
        "movieId": 1,
        "rating": 5
      }
      ```
      
## Services

### CSVWriter

Handles writing rating data to a CSV file stored in HDFS.

- **Initialization:**
  - Initializes the HDFS client using the URL and directory path configured in the Flask app.
  - Ensures the HDFS directory exists and the CSV file has the appropriate headers.

- **Methods:**
  - `__init__()`: Sets up the HDFS client, directory, and file path, and ensures the directory and file headers exist.
  - `_ensure_hdfs_directory_exists()`: Checks if the HDFS directory exists, and creates it if it does not.
  - `_ensure_file_headers()`: Ensures the CSV file has the correct headers. If the file is empty or does not exist, it writes the headers.
  - `write_to_csv(data)`: Appends rating data to the CSV file in HDFS.

### DockerService

Manages Docker containers and Kafka topic creation.

- **Initialization:**
  - Initializes the Docker client.
  - Checks if ZooKeeper and Kafka containers are running.
  - Creates Kafka topics for ratings and recommendations if they do not already exist.

- **Methods:**
  - `__init__()`: Sets up the Docker client and checks for necessary containers and Kafka topics.
  - `check_containers()`: Ensures ZooKeeper and Kafka containers are running.
  - `create_kafka_topic(topic_name)`: Creates a Kafka topic using the provided name if it does not already exist.

### MovieService

Provides operations for managing movie data using MongoDB.

- **Initialization:**
  - Connects to the MongoDB instance and accesses the movie database and collection.

- **Methods:**
  - `__init__()`: Initializes the MongoDB client and connects to the database and collection.
  - `get_all_movies()`: Retrieves all movies from the MongoDB collection. Returns a list of movie documents.

### RatingService

Manages sending and consuming ratings using Kafka, and writing them to HDFS.

- **Initialization:**
  - Sets up Kafka producer and consumer.
  - Initializes the CSVWriter for writing ratings to HDFS.

- **Methods:**
  - `__init__()`: Initializes the Kafka producer, consumer, and CSVWriter.
  - `send_rating(rating_data)`: Sends rating data to the Kafka topic specified for ratings.
  - `consume_ratings()`: Consumes rating messages from Kafka and writes them to HDFS using the CSVWriter.
  - `start_consumer()`: Starts the consumer in a separate thread to continuously process rating messages.

### RecommendationConsumerService

Consumes recommendation messages from Kafka and stores them in Redis.

- **Initialization:**
  - Sets up the Redis client.

- **Methods:**
  - `__init__()`: Initializes the Redis client.
  - `consume_recommendations()`: Consumes recommendation messages from Kafka, processes them, and stores them in Redis.
  - `start_consumer()`: Starts the consumer in a separate thread to continuously process recommendation messages.

### RecommendationService

Generates and manages recommendations using Spark, MongoDB, Redis, and Kafka.

- **Initialization:**
  - Sets up the HDFS client, Spark session, Redis client, MongoDB client, and Kafka producer.

- **Methods:**
  - `__init__()`: Initializes the HDFS client, Spark session, Redis client, MongoDB client, and Kafka producer.
  - `download_ratings_file(local_path)`: Downloads the ratings CSV file from HDFS to a local path.
  - `generate_recommendations_for_all_users(num_recommendations)`: Uses Apache Spark's ALS algorithm to generate movie recommendations for all users. Stores recommendations in MongoDB and sends them to a Kafka topic.
  - `get_recommendations_by_user_id(user_id)`: Retrieves recommendations for a specific user from Redis, and combines them with movie details from MongoDB.

