# 🎬 Movie Data Engineering ETL Pipeline

## 📖 Overview
This project implements a **Movie ETL (Extract, Transform, Load) Pipeline** using **Python, Pandas, SQLite, and Flask**.  
It extracts raw movie data from a CSV file, transforms it into structured tables, loads it into a database, and displays the results through a simple web dashboard.

The goal is to simulate a **real-world data engineering workflow**, from raw data ingestion to data visualization and validation.

---

## ⚙️ Environment Setup & Run Instructions

Follow these steps to set up and run the project locally:

```bash
# 1️⃣ Clone the repository
git clone https://github.com/kumarstationanil-blip/movie-pipeline.git

# 2️⃣ Move into the project folder
cd movie-pipeline

# 3️⃣ Install required dependencies
pip install -r requirements.txt

# 4️⃣ Run the ETL script (optional)
python etl_pipeline.py

# 5️⃣ Start the Flask web app
python app.py
