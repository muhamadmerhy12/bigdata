class Rating:
    def __init__(self, user_id, movie_id, rating):
        self.user_id = user_id
        self.movie_id = movie_id
        self.rating = rating

    def to_dict(self):
        return {
            'userId': self.user_id,
            'movieId': self.movie_id,
            'rating': self.rating
        }
