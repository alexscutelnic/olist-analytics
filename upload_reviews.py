import pandas as pd
from google.cloud import bigquery
import os

# Point to your service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    r"C:\Users\Alexander.Scutelnic.CQDOM\OneDrive - Cubiquity Media Ltd"
    r"\Desktop\My Portfolio\olist_analytics"
    r"\dbt-olist-analytics-9d2d8f25daa0.json"
)

# Load the cleaned CSV
df = pd.read_csv(
    r"C:\Users\Alexander.Scutelnic.CQDOM\OneDrive - Cubiquity Media Ltd"
    r"\Desktop\My Portfolio\Olist Project\olist_order_reviews_clean_v2.csv"
)

print(f"Rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")

# Upload to BigQuery
client = bigquery.Client()

table_id = "dbt-olist-analytics.olist_raw.order_reviews"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # overwrites existing table
)

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # wait for it to finish

print("Done — table uploaded successfully")