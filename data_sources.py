import pandas as pd
import requests
import time

# Step 1: Load Local MovieLens Data
movies = pd.read_csv('movies.csv')
ratings = pd.read_csv('ratings.csv')

print("✅ Loaded MovieLens Data")
print("Movies:", movies.shape)
print("Ratings:", ratings.shape)

# Step 2: Inspect movies
print("\n🎬 Sample Movies Data:")
print(movies.head())

# Step 3: Get OMDb API Data
API_KEY = "YOUR_OMDB_API_KEY_HERE"  # Replace with your own key
API_URL = "http://www.omdbapi.com/"

# Function to fetch details for one movie
def get_movie_details(title):
    params = {"t": title, "apikey": API_KEY}
    try:
        response = requests.get(API_URL, params=params)
        data = response.json()
        if data.get("Response") == "True":
            return {
                "Title": data.get("Title"),
                "Director": data.get("Director"),
                "Plot": data.get("Plot"),
                "BoxOffice": data.get("BoxOffice"),
                "imdbRating": data.get("imdbRating")
            }
        else:
            return {"Title": title, "Director": None, "Plot": None, "BoxOffice": None, "imdbRating": None}
    except Exception as e:
        print("Error:", e)
        return None

# Step 4: Fetch details for first 5 movies (demo)
movie_details = []
for title in movies['title'].head(5):
    print(f"Fetching info for: {title}")
    details = get_movie_details(title)
    movie_details.append(details)
    time.sleep(1)  # Avoid hitting API rate limits

# Step 5: Convert API data to DataFrame
api_df = pd.DataFrame(movie_details)
print("\n✅ OMDb API Data:")
print(api_df)

# Step 6: Merge both datasets
merged = pd.merge(movies.head(5), api_df, left_on='title', right_on='Title', how='left')

print("\n🎯 Merged Data (MovieLens + OMDb):")
print(merged)

# Optional: Save combined data
merged.to_csv('merged_movies.csv', index=False)
print("\n💾 Saved as merged_movies.csv")

