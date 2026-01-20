import os
from datetime import datetime, timezone
from typing import List, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import motor.motor_asyncio
from fastapi.middleware.cors import CORSMiddleware

MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or "mongodb://localhost:27017"
MONGO_DB = os.getenv("MONGO_DB") or os.getenv("MONGODB_DB") or "analytics"

app = FastAPI(title="ELT Analytics API")

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]

# Allow Streamlit dashboard (and others) to call this API from the browser if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8502", "http://localhost:3000", "http://127.0.0.1:8502", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RefreshInfo(BaseModel):
    collection: str
    source_last_modified: Any = None
    ingest_time: Any = None
    delta_source_to_ingest_seconds: float = None
    delta_ingest_to_now_seconds: float = None


@app.get("/collections")
async def list_collections() -> List[str]:
    cols = await db.list_collection_names()
    # filter out internal collections
    return [c for c in cols if not c.startswith("system.")]


@app.get("/collections/{name}")
async def get_collection(name: str, limit: int = 100):
    if name not in await db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found")
    cursor = db[name].find().limit(limit)
    docs = []
    async for d in cursor:
        d["_id"] = str(d.get("_id"))
        docs.append(d)
    return docs


@app.get("/collections/{name}/items")
async def get_collection_items(
    name: str,
    limit: int = 100,
    skip: int = 0,
    filter_field: str | None = None,
    filter_value: str | None = None,
):
    """Return items from a collection with optional simple equality filter, pagination supported."""
    if name not in await db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found")

    query = {}
    if filter_field and filter_value is not None:
        # try to coerce numeric values
        v: object = filter_value
        try:
            if filter_value.isdigit():
                v = int(filter_value)
            else:
                # float?
                v = float(filter_value)
        except Exception:
            v = filter_value
        query[filter_field] = v

    cursor = db[name].find(query).skip(skip).limit(limit)
    docs = []
    async for d in cursor:
        d["_id"] = str(d.get("_id"))
        docs.append(d)
    return {"count": len(docs), "items": docs}


@app.get("/collections/{name}/count")
async def count_collection(name: str):
    if name not in await db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found")
    cnt = await db[name].count_documents({})
    return {"collection": name, "count": cnt}


@app.get("/metadata/{collection}")
async def get_metadata(collection: str):
    meta = await db["ingest_metadata"].find_one({"collection": collection})
    if not meta:
        raise HTTPException(status_code=404, detail="Metadata not found")

    source_lm = meta.get("source_info", {}).get("last_modified")
    ingest_time = meta.get("ingest_time")

    # parse datetimes if possible
    def _to_dt(v):
        if v is None:
            return None
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except Exception:
                return None
        return v

    src_dt = _to_dt(source_lm)
    ing_dt = _to_dt(ingest_time)
    now = datetime.now(timezone.utc)

    delta_src_ing = None
    delta_ing_now = None
    # normalize naive datetimes to UTC for arithmetic
    if src_dt and src_dt.tzinfo is None:
        try:
            src_dt = src_dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    if ing_dt and ing_dt.tzinfo is None:
        try:
            ing_dt = ing_dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    if src_dt and ing_dt:
        delta_src_ing = (ing_dt - src_dt).total_seconds()
    if ing_dt:
        delta_ing_now = (now - ing_dt).total_seconds()

    info = RefreshInfo(
        collection=collection,
        source_last_modified=source_lm,
        ingest_time=ingest_time,
        delta_source_to_ingest_seconds=delta_src_ing,
        delta_ingest_to_now_seconds=delta_ing_now,
    )
    return info.dict()


@app.get("/health")
async def health():
    # simple health check
    try:
        await client.admin.command("ping")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
