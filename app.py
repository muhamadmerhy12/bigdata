from flask import Flask, request, jsonify
from services.movie_service import MovieService
from services.rating_service import rating_service, consume_ratings
from services.recommendation_service import recommendation_service
from threading import Thread

app = Flask(__name__)

# Initialize services
movie_service = MovieService()


@app.route('/movies', methods=['GET'])
def get_movies():
    movies = movie_service.get_all_movies()
    return jsonify(movies)


@app.route('/send_rating', methods=['POST'])
def send_rating():
    rating_data = request.json
    return rating_service.send_rating(rating_data)


@app.route('/recommendations/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    recommendations = recommendation_service.get_recommendations(user_id)
    return jsonify(recommendations)


consumer_thread = Thread(target=consume_ratings, daemon=True)
consumer_thread.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
