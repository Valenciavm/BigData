import io, os
import json
import boto3
import requests as request
from dotenv import load_dotenv

api_urls = {
    "distribuidores_mayorista/": r"https://www.datos.gov.co/api/v3/views/339g-zjac/query.json",    
    "estaciones_servicio/": r"https://www.datos.gov.co/api/v3/views/fbht-2fzd/query.json"
}

load_dotenv()

TOKEN = os.getenv("GOV_TOKEN")
BUCKET = "big-data-lake-energiacol"
S3_PREFIX = "raw/api/"

s3_client = boto3.client("s3")

for path, url in api_urls.items():

    headers = {"X-App-Token": TOKEN}
    res_count = request.get(f"{url}?select=count(*)", headers=headers)
    total_registers = int(res_count.json()[0]["count"])

    limit = 50000
    offset = 0
    chunk_index = 0

    print(f"loading {total_registers} records for {path}")

    while offset < total_registers:
        params = {"$limit": limit, "$offset": offset}

        response = request.get(url, headers=headers, params=params)
        data = response.json()

        if not data:
            break

        json_buffer = io.StringIO()
        json.dump(data, json_buffer)

        s3_key = f"{S3_PREFIX}{path}data_{chunk_index}.json"

        s3_client.put_object(
            Bucket  = BUCKET,
            Key     = s3_key,
            Body    = json_buffer.getvalue()
        )

        print(f"chunk {chunk_index} loaded to S3 [{offset + len(data)}/{total_registers}]")

        offset += limit
        chunk_index += 1

