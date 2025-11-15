import sqlite3
import pandas as pd

conn = sqlite3.connect("people.db")

df = pd.read_sql("PRAGMA table_info(etl_movie_data);", conn)
print(df)

conn.close()
