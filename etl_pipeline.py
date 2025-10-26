import pandas as pd
import requests
import sqlite3
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import time
import re  # for cleaning titles

# -------------------- Load Environment Variables --------------------
load_dotenv()
API_KEY = os.getenv("OMDB_API_KEY")

# -------------------- Database Setup --------------------
DB_PATH = "people.db"
engine = create_engine(f"sqlite:///{DB_PATH}")
print("✅ Connected to database successfully.\n")

# -------------------- Extract Phase --------------------
print("📥 Extracting data from CSV files...")

try:
    movies_df = pd.read_csv("movies.csv")
    ratings_df = pd.read_csv("ratings.csv")
    print("✅ CSV files loaded successfully!\n")
except FileNotFoundError as e:
    print(f"❌ File not found: {e.filename}")
    exit()

# -------------------- API Extraction --------------------
print("🎬 Fetching movie details from OMDb API...\n")

movie_details = []

for title in movies_df["title"].head(10):  # limit to first 10 for demo
    clean_title = re.sub(r"\s*\(\d{4}\)$", "", title)  # remove (year)
    url = f"http://www.omdbapi.com/?t={clean_title}&apikey={API_KEY}"

    try:
        response = requests.get(url)
        data = response.json()

        if data.get("Response") == "True":
            movie_details.append({
                "title": data.get("Title"),
                "year": data.get("Year"),
                "genre": data.get("Genre"),
                "director": data.get("Director"),
                "imdb_rating": data.get("imdbRating"),
                "box_office": data.get("BoxOffice")
            })
        else:
            print(f"⚠️ Movie not found in API: {clean_title}")

        time.sleep(1)  # avoid hitting API rate limits
    except Exception as e:
        print(f"❌ Error fetching {clean_title}: {e}")

# Convert API data to DataFrame
movie_api_df = pd.DataFrame(movie_details)

if movie_api_df.empty:
    print("\n⚠️ No valid movie data fetched from API. Proceeding with CSV data only.\n")
else:
    print("\n✅ Movie details fetched successfully!")
    print(movie_api_df.head(), "\n")

# -------------------- Transform Phase --------------------
print("🧹 Transforming data...")

# Merge ratings with movie info from CSV
merged_df = pd.merge(ratings_df, movies_df, on="movieId", how="left")

# Safe merge with API data
if not movie_api_df.empty:
    final_df = pd.merge(merged_df, movie_api_df, on="title", how="left")
else:
    final_df = merged_df.copy()
    # Add placeholder columns that would have come from API
    final_df["year"] = None
    final_df["genre"] = "Unknown"
    final_df["director"] = "Unknown"
    final_df["imdb_rating"] = "N/A"
    final_df["box_office"] = "N/A"

# Handle missing values
final_df.fillna({
    "imdb_rating": "N/A",
    "box_office": "N/A",
    "director": "Unknown",
    "genre": "Unknown",
    "year": 0
}, inplace=True)

# Add new feature: decade column
def get_decade(year):
    try:
        year = int(year)
        return f"{(year // 10) * 10}s" if year > 0 else "Unknown"
    except:
        return "Unknown"

final_df["decade"] = final_df["year"].apply(get_decade)

print("✅ Transformation complete!\n")
print(final_df.head())

# -------------------- Load Phase --------------------
print("\n💾 Loading data into database...")

with sqlite3.connect(DB_PATH) as conn:
    final_df.to_sql("etl_movie_data", conn, if_exists="replace", index=False)

print("✅ Data loaded into 'etl_movie_data' table successfully!")

# Verify
check_df = pd.read_sql("SELECT * FROM etl_movie_data LIMIT 5", con=engine)
print("\n📊 Preview of data in database:")
print(check_df)
