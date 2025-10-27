from flask import Flask, render_template_string
import sqlite3
import pandas as pd
from etl_pipeline import run_etl  # make sure this file exists and loads data

app = Flask(__name__)

# ---------- HTML Template ----------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>🎬 Movie ETL Dashboard</title>
    <style>
        body { font-family: Arial; background: #f6f6f6; margin: 40px; }
        h1 { color: #2c3e50; }
        a, button {
            display: inline-block;
            padding: 10px 20px;
            margin: 8px;
            color: white;
            background-color: #2980b9;
            border: none;
            border-radius: 6px;
            text-decoration: none;
            font-size: 16px;
        }
        a:hover, button:hover { background-color: #3498db; }
        table { width: 100%; border-collapse: collapse; margin-top: 25px; background: white; }
        th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
        th { background-color: #ecf0f1; }
        p { font-weight: bold; }
    </style>
</head>
<body>
    <h1>🎬 Movie ETL Dashboard</h1>

    <a href="/">🏠 Home</a>
    <a href="/init-db">🧱 Initialize Database</a>
    <a href="/run-etl">⚙️ Run ETL</a>
    <a href="/status">🔍 Check Status</a>
    <a href="/preview">📊 View Table Data</a>

    <p>{{ message }}</p>

    {% if table_html %}
        <h2>Preview of Data</h2>
        {{ table_html|safe }}
    {% endif %}
</body>
</html>
"""

# ---------- ROUTES ----------

@app.route("/")
def home():
    return render_template_string(HTML_TEMPLATE, message="Welcome to Movie ETL Dashboard!", table_html=None)


@app.route("/init-db")
def init_db():
    try:
        conn = sqlite3.connect("people.db")
        with open("schema.sql", "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
        conn.close()
        return render_template_string(HTML_TEMPLATE, message="✅ Database initialized successfully!", table_html=None)
    except Exception as e:
        return render_template_string(HTML_TEMPLATE, message=f"❌ Error: {str(e)}", table_html=None)


@app.route("/run-etl")
def run_etl_route():
    try:
        result = run_etl()
        return render_template_string(HTML_TEMPLATE, message=f"✅ ETL Run Complete: {result}", table_html=None)
    except Exception as e:
        return render_template_string(HTML_TEMPLATE, message=f"❌ ETL Failed: {str(e)}", table_html=None)


@app.route("/status")
def status():
    return render_template_string(HTML_TEMPLATE, message="Server and database running fine ✅", table_html=None)


@app.route("/preview")
def preview():
    try:
        conn = sqlite3.connect("people.db")

        # check if data exists
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM etl_movie_data")
        count = cursor.fetchone()[0]

        if count == 0:
            message = "⚠️ No data found! Please run the ETL process first."
            table_html = None
        else:
            # Fetch top 10 movies by box office
            df = pd.read_sql_query("""
                SELECT movie_id, title, year, imdb_id, box_office, runtime_minutes, director
                FROM etl_movie_data
                ORDER BY box_office DESC
                LIMIT 10;
            """, conn)
            table_html = df.to_html(index=False, classes='data-table')
            message = f"✅ Showing top 10 movies (Total Records: {count})"

        conn.close()
        return render_template_string(HTML_TEMPLATE, message=message, table_html=table_html)

    except Exception as e:
        return render_template_string(HTML_TEMPLATE, message=f"❌ Error loading data: {str(e)}", table_html=None)


# ---------- RUN ----------
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
