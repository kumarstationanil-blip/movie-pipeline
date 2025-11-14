from flask import Flask, render_template_string, send_file, request, redirect, url_for
import sqlite3
import pandas as pd
from io import BytesIO
from etl_pipeline import run_etl  # keep your ETL entrypoint here

app = Flask(__name__)

# ---------- Modern, very different HTML (dark card-based layout, quick stats, small JS filter + CSV export) ----------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Movie ETL — Minimal Dashboard</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>
    :root{--bg:#0f1720;--card:#0b1220;--muted:#98a0ab;--accent:#8ab4ff;--glass:rgba(255,255,255,0.04)}
    html,body{height:100%;margin:0;font-family:Inter,Segoe UI,Arial;background:linear-gradient(180deg,#071021 0%, #0f1720 100%);color:#e6eef8}
    .wrap{max-width:1100px;margin:28px auto;padding:20px}
    header{display:flex;justify-content:space-between;align-items:center}
    h1{margin:0;font-size:20px;letter-spacing:0.2px}
    .actions a, .actions button{background:var(--accent);color:#071021;padding:8px 12px;border-radius:8px;text-decoration:none;margin-left:8px;border:none;cursor:pointer}
    .grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-top:18px}
    .card{background:linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));padding:16px;border-radius:12px;box-shadow:0 6px 18px rgba(3,6,12,0.6);border:1px solid rgba(255,255,255,0.03)}
    .big{grid-column:span 2}
    .stat{font-size:28px;margin:6px 0}
    .muted{color:var(--muted);font-size:13px}
    table{width:100%;border-collapse:collapse;margin-top:12px}
    th,td{padding:10px;border-bottom:1px solid rgba(255,255,255,0.03);font-size:13px}
    th{color:var(--muted);text-align:left}
    .search{width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,0.04);background:var(--card);color:inherit}
    .controls{display:flex;gap:10px;align-items:center;margin-top:12px}
    .small{font-size:12px;color:var(--muted)}
    .table-wrap{max-height:420px;overflow:auto;margin-top:12px}
    footer{margin-top:18px;color:var(--muted);font-size:12px}
    @media (max-width:900px){.grid{grid-template-columns:repeat(1,1fr)}.big{grid-column:span 1}}
  </style>
  <!-- optional Chart.js for a little chart (CDN) -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>🎬 Movie ETL — Modern Dashboard</h1>
        <div class="small">Simple, fast controls for running & previewing your ETL output</div>
      </div>
      <div class="actions">
        <a href="/">Home</a>
        <a href="/init-db">Init DB</a>
        <a href="/run-etl">Run ETL</a>
        <a href="/status">Status</a>
        <a href="/preview">Preview</a>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <div class="muted">Quick Status</div>
        <div id="statusText" class="stat">{{ status_text }}</div>
        <div class="controls">
          <form method="get" action="/run-etl" style="display:inline">
            <button type="submit">⚙️ Run ETL</button>
          </form>
          <a href="/download-csv" class="small">⬇️ Export CSV</a>
          <form method="get" action="/preview" style="display:inline">
            <button type="submit">🔁 Refresh Preview</button>
          </form>
        </div>
      </div>

      <div class="card">
        <div class="muted">Dataset Summary</div>
        <div class="stat">{{ total_records }}</div>
        <div class="muted">Total records in <code>etl_movie_data</code></div>
      </div>

      <div class="card">
        <div class="muted">Top Box Office</div>
        <div class="stat">{{ top_box_office }}</div>
        <div class="muted">Highest box office (rounded)</div>
      </div>

      <div class="card big">
        <div class="muted">Preview — Top movies by box office</div>
        <input id="search" class="search" placeholder="Filter by title or director..." oninput="filterTable()" />
        <div class="table-wrap">
          {{ table_html|safe }}
        </div>
      </div>

      <div class="card">
        <div class="muted">Ratings Chart</div>
        <canvas id="ratingsChart" style="width:100%;height:170px;margin-top:8px"></canvas>
        <div class="muted" style="margin-top:8px">Top 6 movies by average rating</div>
      </div>
    </div>

    <footer>Made with ❤️ — Open your browser console for debug messages.</footer>
  </div>

  <script>
    function filterTable(){
      const q = document.getElementById('search').value.toLowerCase();
      const rows = document.querySelectorAll('.data-table tbody tr');
      rows.forEach(r => {
        const text = r.innerText.toLowerCase();
        r.style.display = text.indexOf(q) === -1 ? 'none' : '';
      });
    }

    // render ratings chart from embedded data
    const chartData = {{ chart_data|safe }};
    const ctx = document.getElementById('ratingsChart');
    if(ctx && chartData.labels.length){
      new Chart(ctx, {
        type: 'bar',
        data: {
          labels: chartData.labels,
          datasets: [{ label: 'Avg rating', data: chartData.values, borderRadius:6 }]
        },
        options: { responsive:true, plugins:{legend:{display:false}} }
      });
    }
  </script>
</body>
</html>
"""

# ---------- Helper functions ----------

def query_db(q, params=()):
    conn = sqlite3.connect('people.db')
    try:
        df = pd.read_sql_query(q, conn, params=params)
    finally:
        conn.close()
    return df


def get_table_count():
    try:
        df = query_db("SELECT COUNT(*) AS cnt FROM etl_movie_data")
        return int(df['cnt'].iloc[0])
    except Exception:
        return 0


def get_top_movies(limit=10):
    try:
        q = """
        SELECT movie_id, title, year, imdb_id, IFNULL(box_office,0) AS box_office, runtime_minutes, director
        FROM etl_movie_data
        ORDER BY box_office DESC
        LIMIT :limit
        """
        return query_db(q, params={'limit': limit})
    except Exception:
        return pd.DataFrame()


def get_top_ratings(limit=6):
    # attempt to detect a rating-like column and compute avg rating per title
    try:
        conn = sqlite3.connect('people.db')
        cols = pd.read_sql_query("PRAGMA table_info('etl_movie_data')", conn)
        conn.close()
        col_names = cols['name'].tolist()
        candidates = [c for c in col_names if any(k in c.lower() for k in ('rating','score','vote','avg','stars'))]
        if not candidates:
            return pd.DataFrame(columns=['title','avg_rating'])
        rating_col = candidates[0]
        q = f"SELECT title, ROUND(AVG(CAST({rating_col} AS FLOAT)),2) AS avg_rating FROM etl_movie_data GROUP BY title ORDER BY avg_rating DESC LIMIT {limit};"
        return query_db(q)
    except Exception:
        return pd.DataFrame(columns=['title','avg_rating'])


# ---------- Routes ----------
@app.route('/')
def home():
    total = get_table_count()
    top = get_top_movies(1)
    top_box = int(top['box_office'].iloc[0]) if not top.empty else 0
    top_box_disp = f"₹{top_box:,}" if top_box else "—"

    preview_df = get_top_movies(10)
    table_html = preview_df.to_html(index=False, classes='data-table') if not preview_df.empty else '<div class="small">No preview available</div>'

    ratings = get_top_ratings(6)
    chart_labels = ratings['title'].tolist()
    chart_values = ratings['avg_rating'].tolist()

    chart_payload = {'labels': chart_labels, 'values': chart_values}

    return render_template_string(HTML_TEMPLATE,
                                  status_text='OK',
                                  total_records=total,
                                  top_box_office=top_box_disp,
                                  table_html=table_html,
                                  chart_data=chart_payload)


@app.route('/init-db')
def init_db():
    try:
        conn = sqlite3.connect('people.db')
        with open('schema.sql','r',encoding='utf-8') as f:
            conn.executescript(f.read())
        conn.commit()
        conn.close()
        return redirect(url_for('home'))
    except Exception as e:
        return f"Init DB error: {e}", 500


@app.route('/run-etl')
def run_etl_route():
    try:
        # run_etl should return a short message or dict; adapt accordingly
        res = run_etl()
        return redirect(url_for('home'))
    except Exception as e:
        return f"ETL error: {e}", 500


@app.route('/status')
def status():
    total = get_table_count()
    return f"Server OK — etl_movie_data records: {total}", 200


@app.route('/preview')
def preview():
    try:
        df = get_top_movies(100)
        if df.empty:
            return redirect(url_for('home'))
        html = df.to_html(index=False, classes='data-table')
        return render_template_string(HTML_TEMPLATE,
                                      status_text='Preview',
                                      total_records=get_table_count(),
                                      top_box_office='—',
                                      table_html=html,
                                      chart_data={'labels':[], 'values':[]})
    except Exception as e:
        return f"Preview error: {e}", 500


@app.route('/download-csv')
def download_csv():
    try:
        df = query_db('SELECT * FROM etl_movie_data')
        if df.empty:
            return "No data to download", 404
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, mimetype='text/csv', as_attachment=True, download_name='etl_movie_data.csv')
    except Exception as e:
        return f"Download error: {e}", 500


# ---------- Run ----------
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
