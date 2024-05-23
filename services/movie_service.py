from pymongo import MongoClient


class MovieService:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['movie_db']
        self.collection = self.db['movies']

    def get_all_movies(self):
        movies = list(self.collection.find({}, {'_id': 0}))
        return movies
