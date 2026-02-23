#!/usr/bin/env python3
"""
Download approved CZDS zone files, parse them (tab-separated: owner, TTL, class, type, rdata),
and upsert into a cloud PostgreSQL database. Intended for GitHub Actions cron.

Required env:
  CZDS_USERNAME, CZDS_PASSWORD  - CZDS credentials
  DATABASE_URL                   - Postgres URL, e.g. postgresql://user:pass@host:5432/dbname

Optional env:
  MAX_TLDS   - Max number of TLDs to process per run (default 10)
  BATCH_SIZE - Rows per insert batch (default 5000)
  TLD_WHITELIST - Comma-separated TLDs to process only (e.g. com,net); if set, MAX_TLDS ignored
"""

import os
import sys
import gzip
import tempfile
from pathlib import Path

# Add repo root so we can import czds_access client helpers
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyczds.client import CZDSClient
import psycopg2
from psycopg2.extras import execute_values


def get_client() -> CZDSClient:
    username = os.environ.get("CZDS_USERNAME")
    password = os.environ.get("CZDS_PASSWORD")
    if not username or not password:
        print("Missing CZDS_USERNAME or CZDS_PASSWORD", file=sys.stderr)
        sys.exit(1)
    return CZDSClient(username, password)


def tld_from_url(url: str) -> str:
    base = url.rstrip("/").split("/")[-1]
    return base.replace(".zone", "") if base.endswith(".zone") else base


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zones (
                id BIGSERIAL PRIMARY KEY,
                tld TEXT NOT NULL UNIQUE,
                serial BIGINT,
                synced_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id BIGSERIAL PRIMARY KEY,
                tld TEXT NOT NULL,
                owner TEXT NOT NULL,
                ttl INT,
                class TEXT,
                type TEXT,
                rdata TEXT,
                synced_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_records_tld ON records(tld);
            CREATE INDEX IF NOT EXISTS idx_records_tld_type ON records(tld, type);
        """)
    conn.commit()


def parse_line(line: str) -> tuple | None:
    """Parse one tab-separated zone line. Returns (owner, ttl, class, type, rdata) or None."""
    line = line.rstrip("\n")
    if not line or line.startswith(";") or line.startswith("$"):
        return None
    parts = line.split("\t")
    if len(parts) < 4:
        return None
    owner, ttl_str, cls, rtype = parts[0], parts[1], parts[2], parts[3]
    rdata = "\t".join(parts[4:]).strip() if len(parts) > 4 else ""
    try:
        ttl = int(ttl_str) if ttl_str else None
    except ValueError:
        ttl = None
    return (owner, ttl, cls or None, rtype or None, rdata or None)


def download_zone(client: CZDSClient, url: str, work_dir: Path) -> Path | None:
    """Download zone file to work_dir. Returns path to .gz or .txt, or None on failure."""
    try:
        client.get_zonefile(url, download_dir=str(work_dir) + os.sep)
    except Exception as e:
        print(f"  Download error: {e}", file=sys.stderr)
        return None
    # Find new file (czds often saves as .zone or .gz)
    for ext in (".gz", ".zone", ".txt"):
        for f in work_dir.glob("*" + ext):
            return f
    return None


def stream_zone_lines(path: Path):
    """Yield parsed (owner, ttl, class, type, rdata) from a .gz or plain file."""
    open_fn = gzip.open if path.suffix == ".gz" else open
    mode = "rt" if path.suffix == ".gz" else "r"
    kwargs = {"encoding": "utf-8", "errors": "replace"} if "t" in mode else {}
    with open_fn(path, mode, **kwargs) as f:
        for line in f:
            row = parse_line(line)
            if row:
                yield row


def sync_tld_to_db(conn, tld: str, path: Path, batch_size: int) -> int:
    """Delete existing records for tld, then insert from path. Returns count inserted."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM records WHERE tld = %s", (tld,))
        deleted = cur.rowcount
    conn.commit()

    inserted = 0
    batch = []
    with conn.cursor() as cur:
        for row in stream_zone_lines(path):
            owner, ttl, cls, rtype, rdata = row
            batch.append((tld, owner, ttl, cls, rtype, rdata))
            if len(batch) >= batch_size:
                execute_values(
                    cur,
                    """
                    INSERT INTO records (tld, owner, ttl, class, type, rdata)
                    VALUES %s
                    """,
                    batch,
                    page_size=batch_size,
                )
                inserted += len(batch)
                batch = []
        if batch:
            execute_values(
                cur,
                """
                INSERT INTO records (tld, owner, ttl, class, type, rdata)
                VALUES %s
                """,
                batch,
                page_size=batch_size,
            )
            inserted += len(batch)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO zones (tld, synced_at) VALUES (%s, NOW()) ON CONFLICT (tld) DO UPDATE SET synced_at = NOW()",
            (tld,),
        )
    conn.commit()
    return inserted


def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("Missing DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    max_tlds = int(os.environ.get("MAX_TLDS", "10"))
    batch_size = int(os.environ.get("BATCH_SIZE", "5000"))
    tld_whitelist_env = os.environ.get("TLD_WHITELIST", "").strip()
    tld_whitelist = [s.strip().lower() for s in tld_whitelist_env.split(",") if s.strip()] if tld_whitelist_env else None

    client = get_client()
    urls = client.get_zonefiles_list()
    if not urls:
        print("No zone files approved for this account.")
        return

    # Build list of (url, tld)
    url_tlds = [(u, tld_from_url(u)) for u in urls]
    if tld_whitelist:
        url_tlds = [(u, t) for u, t in url_tlds if t.lower() in tld_whitelist]
    else:
        url_tlds = url_tlds[:max_tlds]

    if not url_tlds:
        print("No TLDs to process.")
        return

    conn = psycopg2.connect(database_url)
    try:
        ensure_schema(conn)
    finally:
        conn.close()

    with tempfile.TemporaryDirectory(prefix="czds_sync_") as work_dir:
        work_path = Path(work_dir)
        for i, (url, tld) in enumerate(url_tlds, 1):
            print(f"[{i}/{len(url_tlds)}] {tld} ...", flush=True)
            tld_dir = work_path / tld
            tld_dir.mkdir(exist_ok=True)
            path = download_zone(client, url, tld_dir)
            if not path:
                continue
            conn = psycopg2.connect(database_url)
            try:
                n = sync_tld_to_db(conn, tld, path, batch_size)
                print(f"  -> {n} records", flush=True)
            finally:
                conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
