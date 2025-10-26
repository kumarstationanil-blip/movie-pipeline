import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# ------------------- Load Environment Variables -------------------
load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")

if not API_KEY:
    print("❌ ERROR: OMDB_API_KEY not found in .env file.")
    print("👉 Please create a file named '.env' in your project folder and add:")
    print("OMDB_API_KEY=your_api_key_here")
    exit()

# ------------------- Database Setup -------------------
engine = create_engine('sqlite:///people.db')
print("✅ Database connection created successfully.\n")

# ------------------- Load and Transform Local CSV -------------------
try:
    df = pd.read_csv('sample_data.csv')
    print("📄 Original Data:")
    print(df)

    # Example transformation
    df['city'] = df['city'].str.upper()
    print("\n🔄 Transformed Data:")
    print(df)

    df.to_sql('people', con=engine, if_exists='replace', index=False)
    print("\n💾 CSV data successfully loaded into 'people' table.\n")

except FileNotFoundError:
    print("❌ ERROR: sample_data.csv file not found. Please make sure it exists in the same folder.")
    exit()

# ------------------- OMDb API Section -------------------
print("🎬 Fetching movie details from OMDb API...")

movies = ["Inception", "Avatar", "Interstellar", "Titanic", "The Dark Knight"]
movie_data = []

for movie in movies:
    url = f"http://www.omdbapi.com/?t={movie}&apikey={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        if data.get("Response") == "True":
            movie_data.append({
                "Title": data.get("Title"),
                "Year": data.get("Year"),
                "Director": data.get("Director"),
                "Genre": data.get("Genre"),
                "BoxOffice": data.get("BoxOffice"),
                "IMDB_Rating": data.get("imdbRating")
            })
            print(f"✅ Fetched movie: {movie}")
        else:
            print(f"❌ Not Found or API issue: {movie}")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request failed for {movie}: {e}")

# ------------------- Save Movie Data -------------------
if movie_data:
    movie_df = pd.DataFrame(movie_data)
    movie_df.to_sql('movie_details', con=engine, if_exists='replace', index=False)
    print("\n✅ Movie details saved to 'movie_details' table!")
else:
    empty_df = pd.DataFrame(columns=["Title", "Year", "Director", "Genre", "BoxOffice", "IMDB_Rating"])
    empty_df.to_sql('movie_details', con=engine, if_exists='replace', index=False)
    print("\n⚠️ No movie data was saved (check API key or internet connection). Empty table created.")

# ------------------- Verify Data -------------------
print("\n📊 Verifying data saved in database:")

people_df = pd.read_sql('SELECT * FROM people', con=engine)
print("\nPeople Table:")
print(people_df)

movie_df = pd.read_sql('SELECT * FROM movie_details', con=engine)
print("\nMovie Details Table:")
print(movie_df)
