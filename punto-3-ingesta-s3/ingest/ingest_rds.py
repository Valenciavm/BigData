import boto3, pandas as pd
import os, io
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(override=True)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = 3306
DB_NAME = os.getenv("DB_NAME", "energia_col")

database_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(database_url)
s3_client = boto3.client("s3")

BUCKET = "big-data-lake-energiacol"
TABLES = ["productores_b100E", "agentes_gnvc"]

for table in TABLES:
    try:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, engine)
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index = False)

        s3_key = f"raw/mariadb/{table}/table.csv"

        s3_client.put_object(
            Bucket  =BUCKET,
            Key     =s3_key,
            Body    =csv_buffer.getvalue()   
        )

        print(f"table {table} uploaded to s3")
    except Exception as e:
        print(e)
