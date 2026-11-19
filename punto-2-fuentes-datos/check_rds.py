import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "energia_col")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

import pandas as pd

inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tablas encontradas:", tables)

for table in tables:
    print(f"\n--- {table} ---")
    df = pd.read_sql(f"SELECT * FROM `{table}` LIMIT 5", engine)
    print(df.to_string(index=False))
    row_count = pd.read_sql(f"SELECT COUNT(*) as total FROM `{table}`", engine).iloc[0, 0]
    print(f"Total filas: {row_count}")