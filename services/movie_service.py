from pymongo import MongoClient
from flask import current_app as app


class MovieService:
    def __init__(self):
        self.client = MongoClient(app.config['MONGO_URI'])
        self.db = self.client['movie_db']
        self.collection = self.db['movies']

    def get_all_movies(self):
        movies = list(self.collection.find({}, {'_id': 0}))
        return movies
