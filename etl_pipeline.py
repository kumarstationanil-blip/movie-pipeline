import pandas as pd
import sqlite3

def run_etl():
    try:
        # --- Load data from your CSV file ---
        df = pd.read_csv("movies.csv")  # make sure 'movies.csv' is in the same folder
        
        # --- If CSV columns differ, rename to match the DB table ---
        expected_cols = [
            "movie_id", "title", "year", "imdb_id", "box_office", "runtime_minutes", "director"
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None  # add missing columns as blank

        # --- Keep only expected columns ---
        df = df[expected_cols]

        # --- Connect to SQLite database ---
        conn = sqlite3.connect("people.db")
        cursor = conn.cursor()

        # --- Ensure table exists ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_movie_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id TEXT,
                title TEXT,
                year INTEGER,
                imdb_id TEXT,
                box_office BIGINT,
                runtime_minutes INTEGER,
                director TEXT
            )
        """)

        # --- Load data into SQLite table ---
        df.to_sql("etl_movie_data", conn, if_exists="replace", index=False)
        conn.commit()
        conn.close()

        return {"status": "success", "rows_loaded": len(df)}

    except Exception as e:
        return {"status": "error", "message": str(e)}
