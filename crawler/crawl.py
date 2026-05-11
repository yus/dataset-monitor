import os
import sys
import asyncio
import json
import csv
import io
from datetime import datetime, timezone
from pathlib import Path

import httpx
import cchardet
import asyncpg
import yaml

# --- Constants ---
DATABASE_URL = os.environ["DATABASE_URL"]
CATALOGS_FILE = Path(__file__).parent / "catalogs.yaml"
MAX_SAMPLE_ROWS = 5
MAX_SAMPLE_BYTES = 500_000  # 500KB
REQUEST_TIMEOUT = 30
CONCURRENT_LIMIT = 5

# --- DX Scoring ---
def compute_dx(resource):
    """Calculate DX score and tier for a sampled resource."""
    score = 0
    fmt = resource.get("format", "").lower()
    
    if fmt in ("parquet", "avro"):
        score += 30
    elif fmt in ("json", "csv"):
        score += 20
    elif fmt in ("xml",):
        score += 10
    
    if resource.get("http_status") == 200:
        score += 20
    if resource.get("encoding") == "UTF-8":
        score += 10
    if resource.get("has_header", False):
        score += 10
    if resource.get("schema_valid", False):
        score += 15
    
    if score >= 80:
        tier = "gold"
    elif score >= 50:
        tier = "silver"
    elif score >= 1:
        tier = "bronze"
    else:
        tier = "not-machine-readable"
    
    return {"score": score, "tier": tier}

# --- Resource Sampling ---
async def sample_resource(client, url, fmt):
    """Download first bytes of a resource and extract metadata."""
    result = {
        "url": url,
        "format": fmt,
        "http_status": None,
        "detected_mime": None,
        "encoding": None,
        "has_header": False,
        "column_names": [],
        "sample_rows": [],
        "file_size_bytes": 0,
        "schema_valid": False,
        "error": None,
    }
    
    try:
        # HEAD request for size
        head = await client.head(url, timeout=REQUEST_TIMEOUT)
        result["http_status"] = head.status_code
        
        if head.status_code != 200:
            result["error"] = f"HTTP {head.status_code}"
            return result
        
        content_length = head.headers.get("content-length")
        if content_length:
            result["file_size_bytes"] = int(content_length)
        
        # GET first bytes for sampling
        raw = b""
        async with client.stream("GET", url, timeout=REQUEST_TIMEOUT) as resp:
            result["detected_mime"] = resp.headers.get("content-type", "")
            async for chunk in resp.aiter_bytes():
                raw += chunk
                if len(raw) >= MAX_SAMPLE_BYTES:
                    break
        
        if not raw:
            result["error"] = "Empty response body"
            return result
        
        # Detect encoding
        try:
            detected = cchardet.detect(raw)
            result["encoding"] = detected.get("encoding", "unknown")
        except Exception:
            result["encoding"] = "unknown"
        
        # Parse based on format
        fmt_lower = fmt.lower()
        
        if fmt_lower in ("csv", "tsv"):
            try:
                text = raw.decode(result["encoding"] or "utf-8", errors="replace")
                reader = csv.reader(io.StringIO(text))
                first_row = next(reader, [])
                result["column_names"] = first_row
                result["has_header"] = len(first_row) > 0
                result["sample_rows"] = [next(reader, []) for _ in range(MAX_SAMPLE_ROWS)]
                result["schema_valid"] = len(first_row) > 0
            except Exception as e:
                result["error"] = f"CSV parse: {e}"
        
        elif fmt_lower == "json":
            try:
                text = raw.decode(result["encoding"] or "utf-8", errors="replace")
                data = json.loads(text)
                
                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict):
                        result["column_names"] = list(data[0].keys())
                        result["sample_rows"] = [list(d.values()) for d in data[:MAX_SAMPLE_ROWS] if isinstance(d, dict)]
                    result["schema_valid"] = True
                
                elif isinstance(data, dict):
                    result["column_names"] = list(data.keys())
                    result["schema_valid"] = True
            
            except Exception as e:
                result["error"] = f"JSON parse: {e}"
        
        elif fmt_lower in ("parquet", "avro", "xml"):
            result["schema_valid"] = True  # Assume valid, can't parse without libraries
        
        elif fmt_lower in ("pdf", "docx", "doc", "html"):
            result["error"] = "Non-machine-readable format"
    
    except httpx.TimeoutException:
        result["http_status"] = 0
        result["error"] = "Timeout"
    except Exception as e:
        result["http_status"] = 0
        result["error"] = str(e)
    
    return result

# --- Database Operations ---
async def init_db(pool):
    """Ensure tables and view exist."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id              BIGSERIAL PRIMARY KEY,
                source_catalog  TEXT NOT NULL,
                dataset_id      TEXT NOT NULL,
                title           TEXT,
                description     TEXT,
                resource_url    TEXT NOT NULL,
                declared_format TEXT,
                UNIQUE (source_catalog, dataset_id, resource_url)
            );
            
            CREATE TABLE IF NOT EXISTS crawl_snapshots (
                id              BIGSERIAL PRIMARY KEY,
                dataset_id      BIGINT REFERENCES datasets(id) ON DELETE CASCADE,
                checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                http_status     INTEGER,
                detected_mime   TEXT,
                file_size_bytes BIGINT,
                encoding        TEXT,
                has_header      BOOLEAN,
                column_names    JSONB,
                sample_rows     JSONB,
                row_count_est   BIGINT,
                schema_valid    BOOLEAN DEFAULT FALSE,
                dx_score        INTEGER,
                dx_tier         TEXT,
                error_message   TEXT
            );
        """)
        
        # Drop and recreate materialized view
        await conn.execute("DROP MATERIALIZED VIEW IF EXISTS dashboard_latest CASCADE;")
        await conn.execute("""
            CREATE MATERIALIZED VIEW dashboard_latest AS
            SELECT DISTINCT ON (d.id)
                d.id AS dataset_id,
                d.source_catalog,
                d.title,
                d.resource_url,
                d.declared_format,
                s.checked_at,
                s.http_status,
                s.detected_mime,
                s.file_size_bytes,
                s.encoding,
                s.has_header,
                s.column_names,
                s.sample_rows,
                s.dx_score,
                s.dx_tier,
                s.error_message
            FROM datasets d
            JOIN crawl_snapshots s ON s.dataset_id = d.id
            ORDER BY d.id, s.checked_at DESC;
        """)
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ON dashboard_latest (dataset_id);")

async def upsert_dataset(pool, catalog_name, dataset_title, resource_url, fmt):
    """Insert or update dataset, return its ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO datasets (source_catalog, dataset_id, title, resource_url, declared_format)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_catalog, dataset_id, resource_url)
            DO UPDATE SET title = EXCLUDED.title
            RETURNING id
        """, catalog_name, dataset_title, dataset_title, resource_url, fmt)
        return row["id"]

async def insert_snapshot(pool, dataset_id, result, dx):
    """Insert crawl result as a snapshot."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO crawl_snapshots (
                dataset_id, checked_at,
                http_status, detected_mime, file_size_bytes,
                encoding, has_header, column_names, sample_rows,
                schema_valid, dx_score, dx_tier, error_message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """,
            dataset_id,
            datetime.now(timezone.utc),
            result.get("http_status"),
            result.get("detected_mime"),
            result.get("file_size_bytes"),
            result.get("encoding"),
            result.get("has_header"),
            json.dumps(result.get("column_names", [])),
            json.dumps(result.get("sample_rows", [])),
            result.get("schema_valid"),
            dx["score"],
            dx["tier"],
            result.get("error"),
        )

# --- Main ---
async def main():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)
    
    # Load catalogs
    if not CATALOGS_FILE.exists():
        print(f"ERROR: {CATALOGS_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    
    catalogs = yaml.safe_load(CATALOGS_FILE.read_text())
    
    # Connect to database
    try:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=CONCURRENT_LIMIT)
        print("Connected to PostgreSQL.")
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Initialize tables
    await init_db(pool)
    print("Database tables initialized.")
    
    # Crawl all catalogs
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": "DataHealthCrawler/1.0"}
    ) as client:
        
        total_processed = 0
        
        for catalog in catalogs:
            catalog_name = catalog.get("name", "unknown")
            datasets = catalog.get("datasets", [])
            
            print(f"\nCrawling catalog: {catalog_name} ({len(datasets)} datasets)")
            
            for ds in datasets:
                title = ds.get("title", "Untitled")
                resources = ds.get("resources", [])
                
                for resource in resources:
                    url = resource.get("url")
                    fmt = resource.get("format", "unknown")
                    
                    if not url:
                        continue
                    
                    print(f"  Sampling: {url} [{fmt}]")
                    
                    result = await sample_resource(client, url, fmt)
                    dx = compute_dx(result)
                    
                    try:
                        dataset_id = await upsert_dataset(pool, catalog_name, title, url, fmt)
                        await insert_snapshot(pool, dataset_id, result, dx)
                        print(f"    → Score: {dx['score']} ({dx['tier']}), Status: {result.get('http_status')}")
                    except Exception as e:
                        print(f"    → DB insert error: {e}")
                    
                    total_processed += 1
                    
                    # Rate limiting
                    await asyncio.sleep(0.5)
    
    await pool.close()
    print(f"\n✅ Crawl complete. Processed {total_processed} resources.")

if __name__ == "__main__":
    asyncio.run(main())
