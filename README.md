# Canada Employment Analytics — MVP

This is an ongoing project focused on the analysis of Canada's employment in each sector based on NAICS (North American Industry Classification System).

**Stack (MVP):** Python (pandas) → S3/Athena (planned) / Redshift (planned) → dbt (planned) → Streamlit dashboard / Power BI (demo).

## What’s going to be here
- `data/sample_statscan.csv` — small sample dataset (safe, anonymized) for local runs.  
- `etl/etl_local.py` — local ETL (pandas) that cleans sample data and writes `processed.parquet`.  
- `sql/vacancy_rate.sql` — sample SQL to compute vacancy rates per province.  
- `notebooks/` — exploratory notebook scaffold.  
- `dashboard/streamlit_app.py` — placeholder Streamlit app to visualize results.  
- `dbt/` — dbt Core scaffold (for later).
