from pymongo import MongoClient

# Connect to the MongoDB container
client = MongoClient('mongodb://localhost:27017/')

# Select the database and collection
db = client['movie_db']
collection = db['movies']

movies = [
    {"movieId": 1, "title": "Toy Story (1995)", "genre": "Animation|Children's|Comedy"},
    {"movieId": 2, "title": "Jumanji (1995)", "genre": "Adventure|Children's|Fantasy"},
    {"movieId": 3, "title": "Grumpier Old Men (1995)", "genre": "Comedy|Romance"},
    {"movieId": 4, "title": "Waiting to Exhale (1995)", "genre": "Comedy|Drama"},
    {"movieId": 5, "title": "Father of the Bride Part II (1995)", "genre": "Comedy"},
    {"movieId": 6, "title": "Heat (1995)", "genre": "Action|Crime|Thriller"},
    {"movieId": 7, "title": "Sabrina (1995)", "genre": "Comedy|Romance"},
    {"movieId": 8, "title": "Tom and Huck (1995)", "genre": "Adventure|Children's"},
    {"movieId": 9, "title": "Sudden Death (1995)", "genre": "Action"},
    {"movieId": 10, "title": "GoldenEye (1995)", "genre": "Action|Adventure|Thriller"}
]

# Insert the ratings into the collection
collection.insert_many(movies)

print("Ratings inserted successfully.")
