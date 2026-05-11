import os
import httpx
import asyncio
import csv
import io
import json
import yaml
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import cchardet

# --- Config ---
DATABASE_URL = os.environ.get("DATABASE_URL", "")
CATALOGS_FILE = Path("crawler/catalogs.yaml")
MAX_SAMPLE_ROWS = 5
MAX_SAMPLE_BYTES = 500_000

# --- Connect to Supabase & Init Table ---
def init_db():
    if not DATABASE_URL:
        print("No DATABASE_URL, exiting")
        return None
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crawl_snapshots (
            id SERIAL PRIMARY KEY,
            resource_url TEXT,
            catalog_name TEXT,
            format TEXT,
            http_status INTEGER,
            detected_mime TEXT,
            encoding TEXT,
            has_header BOOLEAN,
            column_names JSONB,
            sample_rows JSONB,
            file_size_bytes BIGINT,
            dx_score INTEGER,
            dx_tier TEXT,
            checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_checked_at ON crawl_snapshots(checked_at);
    """)
    conn.commit()
    return conn

# --- DX Scoring ---
def compute_dx(format_, http_status, encoding, has_header):
    fmt = (format_ or "").lower()
    score = 0
    if fmt in ("parquet", "avro"):
        score += 30
    elif fmt in ("json", "csv"):
        score += 20
    elif fmt in ("xml",):
        score += 10
    if http_status == 200:
        score += 20
    if encoding == "UTF-8":
        score += 10
    if has_header:
        score += 10
    tier = "gold" if score >= 70 else "silver" if score >= 40 else "bronze"
    return score, tier

# --- Sample a single resource ---
async def sample(client, url, format_, catalog_name):
    result = {
        "resource_url": url,
        "catalog_name": catalog_name,
        "format": format_,
        "http_status": None,
        "detected_mime": None,
        "encoding": None,
        "has_header": False,
        "column_names": [],
        "sample_rows": [],
        "file_size_bytes": None,
    }
    try:
        head = await client.head(url, timeout=30)
        result["http_status"] = head.status_code
        result["file_size_bytes"] = int(head.headers.get("content-length", 0))
        if head.status_code != 200:
            return result

        async with client.stream("GET", url, timeout=30) as resp:
            raw = b""
            async for chunk in resp.aiter_bytes():
                raw += chunk
                if len(raw) >= MAX_SAMPLE_BYTES:
                    break
            if not raw:
                return result

            detected = cchardet.detect(raw)
            result["encoding"] = detected.get("encoding")

            if format_ and format_.lower() == "csv":
                text = raw.decode(result["encoding"] or "utf-8", errors="replace")
                reader = csv.reader(io.StringIO(text))
                cols = next(reader, [])
                result["column_names"] = cols
                result["has_header"] = bool(cols)
                result["sample_rows"] = [next(reader, []) for _ in range(MAX_SAMPLE_ROWS)]

            elif format_ and format_.lower() == "json":
                data = json.loads(raw.decode(result["encoding"] or "utf-8", errors="replace"))
                if isinstance(data, list) and data:
                    result["column_names"] = list(data[0].keys()) if isinstance(data[0], dict) else []
                    result["sample_rows"] = [list(d.values()) for d in data[:MAX_SAMPLE_ROWS] if isinstance(d, dict)]
    except Exception:
        result["http_status"] = 0
    return result

# --- Main ---
async def main():
    conn = init_db()
    if not conn:
        print("Database connection failed. Check DATABASE_URL.")
        return

    cur = conn.cursor()
    catalogs = yaml.safe_load(CATALOGS_FILE.read_text())
    count = 0

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for catalog in catalogs:
            name = catalog["name"]
            for dataset in catalog["datasets"]:
                resources = dataset.get("resources") or dataset.get("distribution") or []
                for resource in resources:
                    url = resource.get("url") or resource.get("downloadURL")
                    if not url:
                        continue
                    fmt = resource.get("format")
                    result = await sample(client, url, fmt, name)
                    score, tier = compute_dx(fmt, result["http_status"], result["encoding"], result["has_header"])

                    cur.execute("""
                        INSERT INTO crawl_snapshots
                        (resource_url, catalog_name, format, http_status, detected_mime,
                         encoding, has_header, column_names, sample_rows, file_size_bytes,
                         dx_score, dx_tier, checked_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        result["resource_url"],
                        result["catalog_name"],
                        result["format"],
                        result["http_status"],
                        result["detected_mime"],
                        result["encoding"],
                        result["has_header"],
                        json.dumps(result["column_names"]),
                        json.dumps(result["sample_rows"]),
                        result["file_size_bytes"],
                        score,
                        tier,
                        datetime.now(timezone.utc),
                    ))
                    count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Crawl complete. {count} resources saved.")

if __name__ == "__main__":
    asyncio.run(main())
