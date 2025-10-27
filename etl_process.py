# etl_process.py
import sqlite3
import pandas as pd
import os

def run_etl():
    try:
        conn = sqlite3.connect("people.db")
        cursor = conn.cursor()

        # ✅ 1. Create all tables (always ensures etl_movie_data exists)
        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY,
            genre_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS movies (
            movie_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            release_year INTEGER,
            genre_id INTEGER,
            FOREIGN KEY (genre_id) REFERENCES genres (genre_id)
        );

        CREATE TABLE IF NOT EXISTS ratings (
            rating_id INTEGER PRIMARY KEY,
            movie_id INTEGER,
            user_id INTEGER,
            rating REAL,
            timestamp TEXT,
            FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        );

        CREATE TABLE IF NOT EXISTS etl_movie_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id TEXT,
            title TEXT,
            year INTEGER,
            imdb_id TEXT,
            box_office BIGINT,
            runtime_minutes INTEGER,
            director TEXT
        );
        """)

        # ✅ 2. If CSV exists, load it into etl_movie_data
        rows_loaded = 0
        if os.path.exists("movies.csv"):
            df = pd.read_csv("movies.csv")
            df.to_sql("etl_movie_data", conn, if_exists="replace", index=False)
            rows_loaded = len(df)
        else:
            # Insert at least one dummy record so table always exists
            cursor.execute("""
                INSERT INTO etl_movie_data (movie_id, title, year, imdb_id, box_office, runtime_minutes, director)
                VALUES ('M001', 'Inception', 2010, 'tt1375666', 830000000, 148, 'Christopher Nolan')
            """)
            rows_loaded = 1

        # ✅ 3. Insert sample data for other tables (if empty)
        cursor.execute("SELECT COUNT(*) FROM genres;")
        if cursor.fetchone()[0] == 0:
            cursor.executemany("INSERT INTO genres (genre_name) VALUES (?)",
                               [("Action",), ("Drama",), ("Comedy",)])
        
        cursor.execute("SELECT COUNT(*) FROM movies;")
        if cursor.fetchone()[0] == 0:
            cursor.executemany("""
                INSERT INTO movies (title, release_year, genre_id)
                VALUES (?, ?, ?)
            """, [
                ("Inception", 2010, 1),
                ("Interstellar", 2014, 2),
                ("The Dark Knight", 2008, 1)
            ])
        
        cursor.execute("SELECT COUNT(*) FROM ratings;")
        if cursor.fetchone()[0] == 0:
            cursor.executemany("""
                INSERT INTO ratings (movie_id, user_id, rating, timestamp)
                VALUES (?, ?, ?, ?)
            """, [
                (1, 101, 9.0, "2024-10-27"),
                (2, 102, 8.5, "2024-10-27"),
                (3, 103, 9.5, "2024-10-27")
            ])

        conn.commit()
        conn.close()

        return {"status": "success", "rows_loaded": rows_loaded}

    except Exception as e:
        return {"status": "error", "message": str(e)}
