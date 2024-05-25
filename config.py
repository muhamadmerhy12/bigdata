import os


class Config:
    CSV_FILE_PATH = 'ratings.csv'
    DOCKER_KAFKA_CONTAINER_NAME = 'kafka'
    DOCKER_ZOOKEEPER_CONTAINER_NAME = 'zookeeper'
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = os.getenv('REDIS_PORT', 6379)
    REDIS_DB = os.getenv('REDIS_DB', 0)
    KAFKA_RECOMMENDATIONS_TOPIC = 'recommendations'
    KAFKA_RATINGS_TOPIC = 'ratings'
