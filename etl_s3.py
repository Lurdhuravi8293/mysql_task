import pandas as pd
import mysql.connector
import boto3
from datetime import datetime, timedelta
import io

# ---------------- MySQL Connection ----------------
conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="R#vilurdhu8293",
    database="etl_demo"
)

# ---------------- Read All 50 Rows ----------------
df = pd.read_sql("SELECT * FROM transactions ORDER BY id LIMIT 50", conn)
conn.close()

# ---------------- Create S3 Client ----------------
s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIARD4IJBZQKZNSTIFN",
    aws_secret_access_key="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",
    region_name="us-east-1"
)

# ---------------- Process in Chunks of 10 ----------------
bucket_name = "my-wholeproject"
base_path = "daily-uploads"

for i in range(5):
    # Date folder: 5 to 1 days ago
    folder_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
    folder_path = f"{base_path}/{folder_date}/transactions.csv"
    
    # Get 10-row chunk
    df_chunk = df.iloc[i*10:(i+1)*10].copy()
    
    # Add bonus column
    df_chunk["bonus"] = df_chunk["amount"] * 0.10

    # Convert to CSV in memory
    csv_buffer = io.StringIO()
    df_chunk.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=folder_path, Body=csv_buffer.getvalue())

    print(f"Uploaded 10 records to s3://{bucket_name}/{folder_path}")
