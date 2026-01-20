from io import BytesIO
from pathlib import Path

import pandas as pd

from prefect import flow
from .config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows that are completely empty
    df = df.dropna(how="all")

    # Standardize column names
    df.columns = [c.strip() for c in df.columns]

    # Parse any date-like columns
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Normalize numeric columns
    for col in df.select_dtypes(include=["object"]).columns:
        # try to coerce numeric-like columns (montant, id fields)
        if col.lower() in ("montant", "id_achat", "id_client", "id_client"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Lowercase & strip string columns like emails / names
    for col in df.select_dtypes(include=["object"]).columns:
        if any(keyword in col.lower() for keyword in ["email", "nom", "produit", "pays"]):
            df[col] = df[col].astype(str).str.strip()
            if "email" in col.lower():
                df[col] = df[col].str.lower()

    # Remove duplicates
    df = df.drop_duplicates()

    # Final pass: remove rows with critical missing ids
    if "id_client" in df.columns:
        df = df[df["id_client"].notna()]
    if "id_achat" in df.columns:
        df = df[df["id_achat"].notna()]

    return df


def process_object(client, bucket_from: str, object_name: str, bucket_to: str) -> None:
    resp = client.get_object(bucket_from, object_name)
    data = resp.read()
    resp.close()
    resp.release_conn()

    df = pd.read_csv(BytesIO(data))
    df_clean = transform_dataframe(df)

    out = BytesIO()
    df_clean.to_csv(out, index=False)
    out.seek(0)

    client.put_object(bucket_to, object_name, out, length=out.getbuffer().nbytes)
    print(f"Transformed and uploaded {object_name} to {bucket_to}")


@flow(name="Silver Transformation Flow")
def silver_transformation_flow():
    client = get_minio_client()

    # Ensure target bucket exists
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # List objects in bronze and transform each
    objects = client.list_objects(BUCKET_BRONZE, recursive=True)
    for obj in objects:
        name = obj.object_name
        try:
            process_object(client, BUCKET_BRONZE, name, BUCKET_SILVER)
        except Exception as e:
            print(f"Error processing {name}: {e}")


if __name__ == "__main__":
    silver_transformation_flow()
