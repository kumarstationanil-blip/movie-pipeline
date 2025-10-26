import sqlite3

# Connect to your SQLite database (same one you used earlier)
conn = sqlite3.connect("people.db")
cursor = conn.cursor()

# Read schema.sql file safely with UTF-8 encoding
with open("schema.sql", "r", encoding="utf-8") as file:
    sql_script = file.read()

# Execute the schema
cursor.executescript(sql_script)

print("✅ Database schema created successfully!")

conn.commit()
conn.close()
