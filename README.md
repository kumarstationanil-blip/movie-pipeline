<<<<<<< HEAD
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Database](https://img.shields.io/badge/Database-SQLite-orange)
![API](https://img.shields.io/badge/API-OMDb-yellow)

# 🎬 Movie Data Pipeline

This project demonstrates an **ETL (Extract – Transform – Load)** workflow using CSV files, the **OMDb API**, and a **SQLite database**.

---

## 🚀 Project Overview

The goal of this project is to build a small data pipeline that:
1. **Extracts** movie and rating data from CSV files.
2. **Enriches** it with metadata from the OMDb API.
3. **Transforms** the combined dataset for analysis.
4. **Loads** it into a SQLite database for queries.

---

## 🧩 Components

| File | Description |
|------|--------------|
| `create_tables.py` | Creates database tables as defined in `schema.sql`. |
| `etl_pipeline.py` | Main ETL script: reads CSVs, calls OMDb API, merges data, and loads into SQLite. |
| `run_queries.py` | Runs analytical SQL queries on the processed data. |
| `schema.sql` | Defines the database schema (tables, columns, constraints). |
| `.env` | Stores your OMDb API key securely. |
| `movies.csv` / `ratings.csv` | Input CSV datasets used for extraction. |
| `people.db` | SQLite database file generated after running the ETL pipeline. |

---

## ⚙️ Setup Instructions

### 1. Clone or download the project
```bash
git clone https://github.com/yourusername/movie-data-pipeline.git
cd movie-data-pipeline
=======
# movie-pipeline
Data Engineering Assignment – Movie ETL Pipeline
>>>>>>> e5a63175a5f383d37e24470afe71f86421f60d80
