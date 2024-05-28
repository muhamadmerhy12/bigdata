from flask import Flask, jsonify, request
from config import Config
from services.docker_service import DockerService
from services.rating_service import RatingService
from services.recommendation_consumer import RecommendationConsumerService
from services.recommendation_service import RecommendationService


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    with app.app_context():
        DockerService()
        rating_service = RatingService()
        recommendation_service = RecommendationService()
        recommendation_consumer = RecommendationConsumerService()
        recommendation_consumer.start_consumer()
        rating_service.start_consumer()

    @app.route('/recommendations/<int:user_id>', methods=['GET'])
    def get_recommendations_by_user_id(user_id):
        recommendations = recommendation_service.get_recommendations_by_user_id(user_id)
        return jsonify(recommendations)

    @app.route('/send_rating', methods=['POST'])
    def send_rating():
        rating_data = request.json
        return rating_service.send_rating(rating_data)

    return app


if __name__ == '__main__':
    app = create_app()
    app.run()
