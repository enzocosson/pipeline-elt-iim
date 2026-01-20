from io import BytesIO
from datetime import datetime

import pandas as pd

from prefect import flow
from .config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


def read_csv_from_bucket(client, bucket: str, object_name: str) -> pd.DataFrame:
    resp = client.get_object(bucket, object_name)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(BytesIO(data))


def upload_df_to_bucket(client, df: pd.DataFrame, bucket: str, object_name: str) -> None:
    out = BytesIO()
    df.to_csv(out, index=False)
    out.seek(0)
    client.put_object(bucket, object_name, out, length=out.getbuffer().nbytes)
    print(f"Uploaded {object_name} to {bucket}")


def compute_kpis(clients: pd.DataFrame, achats: pd.DataFrame) -> dict:
    # Ensure date column is datetime
    if "date_achat" in achats.columns:
        achats["date_achat"] = pd.to_datetime(achats["date_achat"], errors="coerce")

    # Volumes per period
    achats["day"] = achats["date_achat"].dt.date
    achats["month"] = achats["date_achat"].dt.to_period("M").astype(str)
    achats["week"] = achats["date_achat"].dt.to_period("W").astype(str)

    volumes_day = achats.groupby("day").size().reset_index(name="volume")
    volumes_month = achats.groupby("month").size().reset_index(name="volume")

    # CA par pays: join clients
    joined = achats.merge(clients, left_on="id_client", right_on="id_client", how="left")
    ca_by_country = joined.groupby("pays")["montant"].sum().reset_index().rename(columns={"montant": "ca"})

    # Monthly revenue and growth
    rev_month = joined.groupby("month")["montant"].sum().reset_index().rename(columns={"montant": "revenue"})
    rev_month["revenue"] = rev_month["revenue"].astype(float)
    rev_month = rev_month.sort_values("month")
    rev_month["pct_change"] = rev_month["revenue"].pct_change().fillna(0)

    return {
        "volumes_day": volumes_day,
        "volumes_month": volumes_month,
        "ca_by_country": ca_by_country,
        "monthly_revenue": rev_month,
    }


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow():
    client = get_minio_client()

    # ensure gold bucket
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    # Read silver objects
    objs = list(client.list_objects(BUCKET_SILVER, recursive=True))
    names = [o.object_name for o in objs]

    if "clients.csv" not in names or "achats.csv" not in names:
        raise RuntimeError("Required silver objects clients.csv and achats.csv not found in silver bucket")

    clients = read_csv_from_bucket(client, BUCKET_SILVER, "clients.csv")
    achats = read_csv_from_bucket(client, BUCKET_SILVER, "achats.csv")

    kpis = compute_kpis(clients, achats)

    # Upload KPI results to BUCKET_GOLD
    upload_df_to_bucket(client, kpis["volumes_day"], BUCKET_GOLD, "volumes_day.csv")
    upload_df_to_bucket(client, kpis["volumes_month"], BUCKET_GOLD, "volumes_month.csv")
    upload_df_to_bucket(client, kpis["ca_by_country"], BUCKET_GOLD, "ca_by_country.csv")
    upload_df_to_bucket(client, kpis["monthly_revenue"], BUCKET_GOLD, "monthly_revenue.csv")

    print("Gold aggregations complete.")


if __name__ == "__main__":
    gold_aggregation_flow()
