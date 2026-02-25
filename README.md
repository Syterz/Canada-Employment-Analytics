# Canada Employment Analytics

This is an ongoing project focused on the analysis of Canada's employment in each sector based on NAICS (North American Industry Classification System).

**Stack:** Python (pandas) → S3 / Redshift/Databricks (planned) → dbt (planned) → Streamlit dashboard / Power BI (demo).

## What’s going to be here
- `data/sample_statscan.csv` — small sample dataset (safe, anonymized) for local runs.  
- `etl/etl_local.py` — local ETL (pandas) that cleans sample data and writes `processed.parquet`.
- `glue/glue_etl_job.py` — cloud ETL (Spark) that cleans sample data and writes parquet partitioned based on the Year followed by Canada and its Provinces/Territories .  
- `notebooks/` — exploratory notebook scaffold.  
- `dashboard/streamlit_app.py` — placeholder Streamlit app to visualize results.  
- `dbt/` — dbt Core scaffold (for later).
