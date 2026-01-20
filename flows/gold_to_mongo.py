from io import BytesIO
import os
from datetime import datetime, timezone
from typing import Dict

import pandas as pd
from prefect import flow, task

from .config import BUCKET_GOLD, get_minio_client

MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or "mongodb://localhost:27017"
MONGO_DB = os.getenv("MONGO_DB") or os.getenv("MONGODB_DB") or "analytics"


@task(retries=1)
def read_object_to_df(bucket: str, object_name: str) -> pd.DataFrame:
    """Read object from MinIO into a pandas DataFrame.

    Note: the MinIO client is created inside the task to avoid passing
    non-serializable objects as task inputs (prefect caching/hash warnings).
    """
    client = get_minio_client()
    resp = client.get_object(bucket, object_name)
    data = resp.read()
    resp.close()
    resp.release_conn()

    # detect format
    if object_name.lower().endswith(".parquet"):
        return pd.read_parquet(BytesIO(data))
    else:
        # fallback to csv
        return pd.read_csv(BytesIO(data))


@task(retries=1)
def write_df_to_mongo(df: pd.DataFrame, collection_name: str, metadata: Dict) -> None:
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # drop existing and insert fresh
    if not df.empty:
        records = df.replace({pd.NaT: None}).to_dict(orient="records")
        db[collection_name].delete_many({})
        db[collection_name].insert_many(records)
    else:
        db[collection_name].delete_many({})

    # store ingest metadata (use timezone-aware UTC ISO strings)
    meta = {
        "collection": collection_name,
        "ingest_time": datetime.now(timezone.utc).isoformat(),
        "source_info": {
            **metadata,
            "last_modified": metadata.get("last_modified"),
        },
    }
    db["ingest_metadata"].update_one({"collection": collection_name}, {"$set": meta}, upsert=True)


@flow(name="gold_to_mongo")
def gold_to_mongo_flow():
    client = get_minio_client()

    # ensure there is a gold bucket
    if not client.bucket_exists(BUCKET_GOLD):
        print(f"Bucket {BUCKET_GOLD} not found, nothing to ingest.")
        return

    objs = list(client.list_objects(BUCKET_GOLD, recursive=True))
    if not objs:
        print("No objects found in gold bucket.")
        return

    for obj in objs:
        name = obj.object_name
        lm = getattr(obj, 'last_modified', None)
        print(f"Processing {name} (last_modified={lm})")
        try:
            df = read_object_to_df(BUCKET_GOLD, name)
            metadata = {
                "object_name": name,
                "last_modified": lm,
            }
            collection = os.path.splitext(name)[0]
            write_df_to_mongo(df, collection, metadata)
            print(f"Wrote {len(df)} records to MongoDB collection '{collection}'")
        except Exception as e:
            print(f"Failed to process {name}: {e}")


if __name__ == "__main__":
    gold_to_mongo_flow()
