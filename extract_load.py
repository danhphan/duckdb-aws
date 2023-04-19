import duckdb as db
from utils import upload_file

# Set up duckdb connection
conn = db.connect()
conn.execute("INSTALL httpfs;")


file_names = ["jaffle_shop_orders", "jaffle_shop_customers", "stripe_payments"]
for file_name in file_names:
    df = conn.sql(f"SELECT * FROM read_csv('s3://dbt-tutorial-public/{file_name}.csv', AUTO_DETECT=TRUE)")
    df.write_parquet(f'./data/{file_name}.parquet')
    upload_file(f'./data/{file_name}.parquet', "dbt-jaffle-shop")
    print(f"Successfully uploaded {file_name}.parquet into s3://dbt-jaffle-shop bucket")