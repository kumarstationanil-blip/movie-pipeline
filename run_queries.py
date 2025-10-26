import sqlite3
import pandas as pd

# Connect to the database created in ETL
conn = sqlite3.connect("people.db")

# Define all the analytical queries
queries = [
    # 1️⃣ Highest average rated movie
    "SELECT title, ROUND(AVG(rating), 2) AS avg_rating FROM etl_movie_data GROUP BY title ORDER BY avg_rating DESC LIMIT 1;",

    # 2️⃣ Top 5 genres with highest average rating
    "SELECT genre, ROUND(AVG(rating), 2) AS avg_rating FROM etl_movie_data WHERE genre IS NOT NULL AND genre != 'Unknown' GROUP BY genre ORDER BY avg_rating DESC LIMIT 5;",

    # 3️⃣ Director with most movies
    "SELECT director, COUNT(DISTINCT title) AS total_movies FROM etl_movie_data WHERE director IS NOT NULL AND director != 'Unknown' GROUP BY director ORDER BY total_movies DESC LIMIT 1;",

    # 4️⃣ Average rating per release year
    "SELECT year, ROUND(AVG(rating), 2) AS avg_rating FROM etl_movie_data WHERE year IS NOT NULL AND year != 'Unknown' GROUP BY year ORDER BY year ASC;"
]

# Execute and display results
for i, q in enumerate(queries, start=1):
    print(f"\n📊 Query {i} Result:")
    df = pd.read_sql(q, conn)
    print(df, "\n")

conn.close()
print("✅ All queries executed successfully!")
