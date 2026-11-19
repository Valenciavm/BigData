import asyncio
import io, os
import json
import time
import boto3
import requests as request
from dotenv import load_dotenv

load_dotenv(override=True)

API_URLS = {
    "distribuidores_mayorista/": "https://www.datos.gov.co/resource/339g-zjac.json",
    "estaciones_servicio/":      "https://www.datos.gov.co/resource/fbht-2fzd.json"
}

TOKEN     = os.getenv("GOV_TOKEN")
BUCKET    = "big-data-lake-energiacol"
S3_PREFIX = "raw/api/"

# ── Per-dataset ingestion (runs in a thread) ─────────────────────────────────
def ingest_dataset(path: str, url: str) -> None:
    """Fetch all pages of one Socrata endpoint and upload each chunk to S3."""
    s3_client = boto3.client("s3")
    headers   = {"X-App-Token": TOKEN}

    # Find already uploaded chunks in S3 to resume safely
    existing_chunks = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{S3_PREFIX}{path}"):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if fname.startswith("data_") and fname.endswith(".json"):
                try:
                    idx = int(fname[len("data_"):-len(".json")])
                    existing_chunks.add(idx)
                except ValueError:
                    pass

    if existing_chunks:
        print(f"[{path}] found {len(existing_chunks)} existing chunk(s) in S3 (resuming)...")

    # Known counts as fallback in case Socrata count(*) query is slow/times out
    fallback_counts = {
        "distribuidores_mayorista/": 7197613,
        "estaciones_servicio/":      6496164
    }

    try:
        print(f"[{path}] querying record count from {url}...")
        res_count = request.get(url, params={"$select": "count(*)"}, headers=headers, timeout=15)
        total_registers = int(res_count.json()[0]["count"])
    except Exception:
        total_registers = fallback_counts.get(path, 7000000)
        print(f"[{path}] count query timed out; using baseline estimate: {total_registers:,}")

    limit       = 50000
    offset      = 0
    chunk_index = 0

    print(f"[{path}] starting — {total_registers:,} total records (chunks of {limit:,})")

    while offset < total_registers:
        # If already in S3, skip downloading
        if chunk_index in existing_chunks:
            offset      += limit
            chunk_index += 1
            continue

        params = {"$limit": limit, "$offset": offset}
        data = None

        # Retry with backoff for network timeouts
        for attempt in range(1, 5):
            try:
                response = request.get(url, headers=headers, params=params, timeout=120)
                data = response.json()
                break
            except Exception as e:
                print(f"[{path}] chunk {chunk_index} (attempt {attempt}/4) error: {e}. Retrying in 5s...")
                time.sleep(5)

        if data is None:
            raise RuntimeError(f"[{path}] failed to fetch chunk {chunk_index} after 4 attempts")

        if not data:
            break

        json_buffer = io.StringIO()
        json.dump(data, json_buffer)

        s3_key = f"{S3_PREFIX}{path}data_{chunk_index}.json"
        s3_client.put_object(
            Bucket = BUCKET,
            Key    = s3_key,
            Body   = json_buffer.getvalue()
        )

        offset      += limit
        chunk_index += 1
        print(f"[{path}] chunk {chunk_index} uploaded — {min(offset, total_registers):,}/{total_registers:,}")

    print(f"[{path}] done — {chunk_index} total chunks confirmed in S3")

# ── Async runner ─────────────────────────────────────────────────────────────
async def main() -> None:
    tasks = [
        asyncio.to_thread(ingest_dataset, path, url)
        for path, url in API_URLS.items()
    ]
    await asyncio.gather(*tasks)
    print("\nAll API datasets ingested.")

if __name__ == "__main__":
    asyncio.run(main())
