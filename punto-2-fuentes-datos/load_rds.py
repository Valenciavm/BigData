#En este script cargar los datos a MariaDB, cargar Productores B100/Etanol y Agentes GNCV

import pandas as pd
import os
import dotenv
from sqlalchemy import create_engine, text

dotenv.load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = 3306
DB_NAME = "energia_col"

# Create the database if it doesn't exist
engine_root = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/'
)
with engine_root.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
engine_root.dispose()

engine = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

df_b100E = pd.read_csv(os.path.join(BASE_DIR, "datos", "Productores_Productores_de_B100_y_Etanol_-_Alcohol_Carburante_(AUTOMATIZADO)_20261114.csv"))
df_gnvc = pd.read_csv(os.path.join(BASE_DIR, "datos", "Consulta_Agentes_de_Gas_Natural_Comprimido_Vehicular_(AUTOMATIZADO)_20261114.csv"))

df_b100E.to_sql(
    name = "productores_b100E", con=engine, if_exists = "replace", index = False, chunksize = 1000
) 

df_gnvc.to_sql(
    name = "agentes_gnvc", con=engine, if_exists="replace",  index = False, chunksize = 1000 
)

print("success data loaded to mariadb")









