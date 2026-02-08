#!/usr/bin/env python
"""
NYC TLC Trip Record Data Ingestion

Downloads parquet files from NYC's TLC website and loads them into PostgreSQL.

PostgreSQL connection is configured via environment variables:
    POSTGRES_HOST     (default: localhost)
    POSTGRES_PORT     (default: 5432)
    POSTGRES_DB       (default: nyc_taxi)
    POSTGRES_USER     (default: postgres)
    POSTGRES_PASSWORD (default: empty)

Usage examples:
    # Download yellow taxi data for Jan-Mar 2024 and load into PostgreSQL
    python ingest.py --types yellow --start 2024-01 --end 2024-03

    # Download all taxi types for 2023, download only (no DB load)
    python ingest.py --start 2023-01 --end 2023-12 --download-only

    # Load previously downloaded files into PostgreSQL
    python ingest.py --types yellow --start 2024-01 --end 2024-03 --no-download

    # Parallel ingestion: 2 concurrent downloads, 24 DB load workers
    python ingest.py --types yellow green --start 2024-01 --end 2024-12 --download-workers 2 --workers 24
"""

import argparse
import io
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
DATA_DIR = Path(__file__).parent / "data"

# Arrow type string prefix → PostgreSQL type
ARROW_TO_PG = {
    "int8": "INTEGER",
    "int16": "INTEGER",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "string": "TEXT",
    "large_string": "TEXT",
    "timestamp": "TIMESTAMP",
    "date32": "DATE",
    "date64": "DATE",
    "bool": "BOOLEAN",
}

# Yellow taxi canonical schema: (column_name, pg_type, arrow_type)
# Uses oldest names; includes columns added in later eras (NULL-filled for older data)
YELLOW_COLUMNS = [
    ("vendor_name", "TEXT", pa.string()),
    ("trip_pickup_datetime", "TIMESTAMP", pa.string()),
    ("trip_dropoff_datetime", "TIMESTAMP", pa.string()),
    ("passenger_count", "BIGINT", pa.int64()),
    ("trip_distance", "DOUBLE PRECISION", pa.float64()),
    ("start_lon", "DOUBLE PRECISION", pa.float64()),
    ("start_lat", "DOUBLE PRECISION", pa.float64()),
    ("rate_code", "TEXT", pa.string()),
    ("store_and_forward", "TEXT", pa.string()),
    ("end_lon", "DOUBLE PRECISION", pa.float64()),
    ("end_lat", "DOUBLE PRECISION", pa.float64()),
    ("payment_type", "TEXT", pa.string()),
    ("fare_amt", "DOUBLE PRECISION", pa.float64()),
    ("surcharge", "DOUBLE PRECISION", pa.float64()),
    ("mta_tax", "DOUBLE PRECISION", pa.float64()),
    ("tip_amt", "DOUBLE PRECISION", pa.float64()),
    ("tolls_amt", "DOUBLE PRECISION", pa.float64()),
    ("total_amt", "DOUBLE PRECISION", pa.float64()),
    ("pulocationid", "BIGINT", pa.int64()),
    ("dolocationid", "BIGINT", pa.int64()),
    ("improvement_surcharge", "DOUBLE PRECISION", pa.float64()),
    ("congestion_surcharge", "DOUBLE PRECISION", pa.float64()),
    ("airport_fee", "DOUBLE PRECISION", pa.float64()),
    ("cbd_congestion_fee", "DOUBLE PRECISION", pa.float64()),
]

YELLOW_CANONICAL_NAMES = [c[0] for c in YELLOW_COLUMNS]

# Maps parquet column names (lowercased) to canonical names
YELLOW_RENAMES = {
    # 2010+ renames
    "vendor_id": "vendor_name",
    "pickup_datetime": "trip_pickup_datetime",
    "dropoff_datetime": "trip_dropoff_datetime",
    "pickup_longitude": "start_lon",
    "pickup_latitude": "start_lat",
    "dropoff_longitude": "end_lon",
    "dropoff_latitude": "end_lat",
    "fare_amount": "fare_amt",
    "tip_amount": "tip_amt",
    "tolls_amount": "tolls_amt",
    "total_amount": "total_amt",
    "store_and_fwd_flag": "store_and_forward",
    # 2011+ renames
    "vendorid": "vendor_name",
    "tpep_pickup_datetime": "trip_pickup_datetime",
    "tpep_dropoff_datetime": "trip_dropoff_datetime",
    "ratecodeid": "rate_code",
    "extra": "surcharge",
}

YELLOW_DROP = {"__index_level_0__"}


def normalize_yellow(table):
    """Normalize a yellow taxi Arrow table to the canonical schema."""
    # Lowercase column names
    cols = {name.lower(): table.column(name) for name in table.schema.names}

    # Drop junk columns
    for drop in YELLOW_DROP:
        cols.pop(drop, None)

    # Rename columns
    renamed = {}
    for old_name, data in cols.items():
        new_name = YELLOW_RENAMES.get(old_name, old_name)
        renamed[new_name] = data

    # Build canonical table: use existing column or null-fill
    nrows = table.num_rows
    arrays = []
    names = []
    for col_name, _, arrow_type in YELLOW_COLUMNS:
        if col_name in renamed:
            col = renamed[col_name]
            # Cast to canonical type if needed (e.g., int→string for vendor)
            if col.type != arrow_type:
                col = col.cast(arrow_type, safe=False)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(nrows, type=arrow_type))
        names.append(col_name)

    return pa.table(arrays, names=names)


# Green taxi canonical schema: (column_name, pg_type, arrow_type)
# Consistent column names across all eras; cbd_congestion_fee added in 2025
GREEN_COLUMNS = [
    ("vendorid", "BIGINT", pa.int64()),
    ("lpep_pickup_datetime", "TIMESTAMP", pa.string()),
    ("lpep_dropoff_datetime", "TIMESTAMP", pa.string()),
    ("store_and_fwd_flag", "TEXT", pa.string()),
    ("ratecodeid", "BIGINT", pa.int64()),
    ("pulocationid", "BIGINT", pa.int64()),
    ("dolocationid", "BIGINT", pa.int64()),
    ("passenger_count", "BIGINT", pa.int64()),
    ("trip_distance", "DOUBLE PRECISION", pa.float64()),
    ("fare_amount", "DOUBLE PRECISION", pa.float64()),
    ("extra", "DOUBLE PRECISION", pa.float64()),
    ("mta_tax", "DOUBLE PRECISION", pa.float64()),
    ("tip_amount", "DOUBLE PRECISION", pa.float64()),
    ("tolls_amount", "DOUBLE PRECISION", pa.float64()),
    ("ehail_fee", "DOUBLE PRECISION", pa.float64()),
    ("improvement_surcharge", "DOUBLE PRECISION", pa.float64()),
    ("total_amount", "DOUBLE PRECISION", pa.float64()),
    ("payment_type", "BIGINT", pa.int64()),
    ("trip_type", "BIGINT", pa.int64()),
    ("congestion_surcharge", "DOUBLE PRECISION", pa.float64()),
    ("cbd_congestion_fee", "DOUBLE PRECISION", pa.float64()),
]


# FHV canonical schema
FHV_COLUMNS = [
    ("dispatching_base_num", "TEXT", pa.string()),
    ("pickup_datetime", "TIMESTAMP", pa.string()),
    ("dropoff_datetime", "TIMESTAMP", pa.string()),
    ("pulocationid", "DOUBLE PRECISION", pa.float64()),
    ("dolocationid", "DOUBLE PRECISION", pa.float64()),
    ("sr_flag", "TEXT", pa.string()),
    ("affiliated_base_number", "TEXT", pa.string()),
]

# FHVHV canonical schema
FHVHV_COLUMNS = [
    ("hvfhs_license_num", "TEXT", pa.string()),
    ("dispatching_base_num", "TEXT", pa.string()),
    ("originating_base_num", "TEXT", pa.string()),
    ("request_datetime", "TIMESTAMP", pa.string()),
    ("on_scene_datetime", "TIMESTAMP", pa.string()),
    ("pickup_datetime", "TIMESTAMP", pa.string()),
    ("dropoff_datetime", "TIMESTAMP", pa.string()),
    ("pulocationid", "BIGINT", pa.int64()),
    ("dolocationid", "BIGINT", pa.int64()),
    ("trip_miles", "DOUBLE PRECISION", pa.float64()),
    ("trip_time", "BIGINT", pa.int64()),
    ("base_passenger_fare", "DOUBLE PRECISION", pa.float64()),
    ("tolls", "DOUBLE PRECISION", pa.float64()),
    ("bcf", "DOUBLE PRECISION", pa.float64()),
    ("sales_tax", "DOUBLE PRECISION", pa.float64()),
    ("congestion_surcharge", "DOUBLE PRECISION", pa.float64()),
    ("airport_fee", "DOUBLE PRECISION", pa.float64()),
    ("tips", "DOUBLE PRECISION", pa.float64()),
    ("driver_pay", "DOUBLE PRECISION", pa.float64()),
    ("shared_request_flag", "TEXT", pa.string()),
    ("shared_match_flag", "TEXT", pa.string()),
    ("access_a_ride_flag", "TEXT", pa.string()),
    ("wav_request_flag", "TEXT", pa.string()),
    ("wav_match_flag", "TEXT", pa.string()),
]


def _normalize_generic(table, columns):
    """Generic normalizer: cast types and null-fill missing columns."""
    cols = {name.lower(): table.column(name) for name in table.schema.names}
    nrows = table.num_rows
    arrays = []
    names = []
    for col_name, _, arrow_type in columns:
        if col_name in cols:
            col = cols[col_name]
            if col.type != arrow_type:
                col = col.cast(arrow_type, safe=False)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(nrows, type=arrow_type))
        names.append(col_name)
    return pa.table(arrays, names=names)


def normalize_green(table):
    return _normalize_generic(table, GREEN_COLUMNS)


def normalize_fhv(table):
    return _normalize_generic(table, FHV_COLUMNS)


def normalize_fhvhv(table):
    return _normalize_generic(table, FHVHV_COLUMNS)


NORMALIZERS = {
    "yellow": normalize_yellow,
    "green": normalize_green,
    "fhv": normalize_fhv,
    "fhvhv": normalize_fhvhv,
}

CANONICAL_SCHEMAS = {
    "yellow": YELLOW_COLUMNS,
    "green": GREEN_COLUMNS,
    "fhv": FHV_COLUMNS,
    "fhvhv": FHVHV_COLUMNS,
}


# Lock for thread-safe printing
_print_lock = threading.Lock()

# Shared rate-limit cooldown: all download threads check this before requesting
_rate_limit_lock = threading.Lock()
_rate_limit_until = 0.0  # monotonic timestamp


def log(msg):
    with _print_lock:
        print(msg, flush=True)


def get_db_params(args):
    """Build a dict of psycopg2 connection params from CLI args / env vars."""
    return {
        "host": args.db_host or os.environ.get("POSTGRES_HOST", "localhost"),
        "port": args.db_port or os.environ.get("POSTGRES_PORT", "5432"),
        "dbname": args.db_name or os.environ.get("POSTGRES_DB", "nyc_taxi"),
        "user": args.db_user or os.environ.get("POSTGRES_USER", "postgres"),
        "password": args.db_password or os.environ.get("POSTGRES_PASSWORD", ""),
    }


def generate_months(start, end):
    """Yield YYYY-MM strings from start to end inclusive."""
    current = datetime.strptime(start, "%Y-%m")
    end_dt = datetime.strptime(end, "%Y-%m")
    while current <= end_dt:
        yield current.strftime("%Y-%m")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def download_file(url, dest, quiet=False, cooldown=30, max_retries=5):
    """Download a file with 403 backoff. Returns True on success, False if 404/missing."""
    global _rate_limit_until
    headers = {"User-Agent": "curl/8.0"}

    for attempt in range(max_retries + 1):
        # Wait if a rate-limit cooldown is active
        while True:
            with _rate_limit_lock:
                wait_until = _rate_limit_until
            remaining = wait_until - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 5))  # check periodically

        resp = requests.get(url, stream=True, headers=headers)

        if resp.status_code == 404:
            return False

        if resp.status_code == 403:
            if attempt == 0:
                # First 403: could be rate limiting or missing file. Set cooldown and retry.
                with _rate_limit_lock:
                    now = time.monotonic()
                    if now >= _rate_limit_until:
                        _rate_limit_until = now + cooldown
                        log(f"  Got 403, pausing {cooldown}s before retry...")
                continue
            else:
                # Still 403 after cooldown: file genuinely doesn't exist
                return False

        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        dest.parent.mkdir(parents=True, exist_ok=True)

        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if not quiet and total:
                    pct = downloaded / total * 100
                    mb = downloaded / 1024 / 1024
                    total_mb = total / 1024 / 1024
                    print(f"\r  {mb:.1f}/{total_mb:.1f} MB ({pct:.0f}%)", end="", flush=True)
        if not quiet:
            print()
        return True

    log(f"  Failed after {max_retries} retries: {url}")
    return False


def normalize_col(name):
    """Normalize a column name to lowercase with underscores."""
    return name.strip().lower().replace(" ", "_")


def arrow_type_to_pg(arrow_type):
    """Map an Arrow type to a PostgreSQL type string."""
    type_str = str(arrow_type)
    for prefix, pg_type in ARROW_TO_PG.items():
        if type_str.startswith(prefix):
            return pg_type
    return "TEXT"


def create_table_from_schema(cursor, table_name, schema):
    """Create a PostgreSQL table from an Arrow schema if it doesn't exist."""
    columns = []
    for field in schema:
        col_name = normalize_col(field.name)
        pg_type = arrow_type_to_pg(field.type)
        columns.append(f'"{col_name}" {pg_type}')
    cols_sql = ", ".join(columns)
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_sql})')


def ensure_tables(db_params, taxi_types, data_dir):
    """Pre-create tables using canonical schema (if defined) or first parquet file."""
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    for taxi_type in taxi_types:
        table_name = f"{taxi_type}_tripdata"
        if taxi_type in CANONICAL_SCHEMAS:
            cols_sql = ", ".join(
                f'"{name}" {pg_type}' for name, pg_type, _ in CANONICAL_SCHEMAS[taxi_type]
            )
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_sql})')
        else:
            files = sorted(data_dir.glob(f"{taxi_type}_tripdata_*.parquet"))
            if not files:
                continue
            schema = pq.read_schema(files[0])
            create_table_from_schema(cur, table_name, schema)
        conn.commit()
    cur.close()
    conn.close()


def load_to_postgres(filepath, conn, table_name, batch_size=100_000, quiet=False, normalizer=None):
    """Read a parquet file and load it into PostgreSQL via COPY. Returns row count."""
    pf = pq.ParquetFile(filepath)
    total_rows = pf.metadata.num_rows

    if not quiet:
        print(f"  Loading {total_rows:,} rows into '{table_name}'...")

    write_opts = pa_csv.WriteOptions(include_header=False)
    cur = conn.cursor()

    # Determine column names for COPY: use normalizer's canonical schema or raw file schema
    if normalizer:
        # Read one batch to get normalized column names
        first_batch = next(pf.iter_batches(batch_size=1))
        sample = normalizer(pa.table(first_batch.columns, names=first_batch.schema.names))
        col_names = ", ".join(f'"{c}"' for c in sample.column_names)
        # Re-open file to iterate from the start
        pf = pq.ParquetFile(filepath)
    else:
        schema = pf.schema_arrow
        col_names = ", ".join(f'"{normalize_col(f.name)}"' for f in schema)
        create_table_from_schema(cur, table_name, schema)
        conn.commit()

    loaded = 0
    for batch in pf.iter_batches(batch_size=batch_size):
        table = pa.table(batch.columns, names=batch.schema.names)
        if normalizer:
            table = normalizer(table)
        buf = io.BytesIO()
        pa_csv.write_csv(table, buf, write_options=write_opts)
        buf.seek(0)
        cur.copy_expert(
            f"""COPY "{table_name}" ({col_names}) FROM STDIN WITH (FORMAT CSV, NULL '')""",
            buf,
        )
        conn.commit()
        loaded += batch.num_rows
        if not quiet:
            print(f"\r  {loaded:,}/{total_rows:,} rows", end="", flush=True)

    cur.close()
    if not quiet:
        print(" - done")
    return total_rows


def process_file(taxi_type, month, args, db_params, dl_semaphore):
    """Download and load a single file. Returns (taxi_type, month, rows, status, elapsed)."""
    table_name = f"{taxi_type}_tripdata"
    filename = f"{taxi_type}_tripdata_{month}.parquet"
    filepath = args.data_dir / filename
    url = f"{BASE_URL}/{filename}"
    parallel = args.workers > 1
    t0 = time.monotonic()

    # Download (gated by semaphore to avoid CDN rate limiting)
    if not args.no_download:
        if filepath.exists():
            size_mb = filepath.stat().st_size / 1024 / 1024
            if not parallel:
                print(f"  Already downloaded ({size_mb:.1f} MB), skipping")
        else:
            dl_semaphore.acquire()
            try:
                if not parallel:
                    print(f"  Downloading {url}")
                ok = download_file(url, filepath, quiet=parallel)
            finally:
                dl_semaphore.release()
            if not ok:
                elapsed = time.monotonic() - t0
                if parallel:
                    log(f"  [{taxi_type} {month}] not found on server, skipping")
                else:
                    print("  Not found on server, skipping")
                return (taxi_type, month, 0, "not_found", elapsed)

    # Load to DB
    rows = 0
    if not args.download_only:
        if not filepath.exists():
            elapsed = time.monotonic() - t0
            if parallel:
                log(f"  [{taxi_type} {month}] file missing, skipping")
            else:
                print(f"  File not found: {filepath}, skipping")
            return (taxi_type, month, 0, "missing", elapsed)
        try:
            normalizer = NORMALIZERS.get(taxi_type)
            conn = psycopg2.connect(**db_params)
            try:
                rows = load_to_postgres(filepath, conn, table_name, args.batch_size, quiet=parallel, normalizer=normalizer)
            finally:
                conn.close()
        except Exception as e:
            elapsed = time.monotonic() - t0
            if parallel:
                log(f"  [{taxi_type} {month}] error: {e}")
            else:
                print(f"  Error loading: {e}", file=sys.stderr)
            return (taxi_type, month, 0, "error", elapsed)

    elapsed = time.monotonic() - t0
    return (taxi_type, month, rows, "ok", elapsed)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Download NYC TLC taxi trip data and load into PostgreSQL.",
    )
    p.add_argument(
        "--types",
        nargs="+",
        choices=TAXI_TYPES,
        default=TAXI_TYPES,
        help="Taxi types to download (default: all)",
    )
    p.add_argument(
        "--start",
        required=True,
        help="Start month in YYYY-MM format",
    )
    p.add_argument(
        "--end",
        required=True,
        help="End month in YYYY-MM format",
    )
    p.add_argument(
        "--download-only",
        action="store_true",
        help="Download parquet files without loading into PostgreSQL",
    )
    p.add_argument(
        "--no-download",
        action="store_true",
        help="Skip download, load existing local files into PostgreSQL",
    )
    p.add_argument(
        "--replace",
        action="store_true",
        help="Drop and recreate tables instead of appending",
    )
    p.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help=f"Directory for downloaded parquet files (default: {DATA_DIR})",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per COPY batch (default: 100000)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel DB load workers (default: 4)",
    )
    p.add_argument(
        "--download-workers",
        type=int,
        default=2,
        help="Max concurrent downloads (default: 2)",
    )

    db = p.add_argument_group("database", "PostgreSQL connection (overrides env vars)")
    db.add_argument("--db-host", default=None)
    db.add_argument("--db-port", default=None)
    db.add_argument("--db-name", default=None)
    db.add_argument("--db-user", default=None)
    db.add_argument("--db-password", default=None)

    return p.parse_args(argv)


def main():
    args = parse_args()
    months = list(generate_months(args.start, args.end))
    parallel = args.workers > 1

    print(f"Taxi types: {', '.join(args.types)}")
    print(f"Date range: {args.start} to {args.end} ({len(months)} months)")
    print(f"Total files: {len(args.types) * len(months)}")
    print(f"Workers: {args.workers} (download: {args.download_workers})")
    print()

    # Resolve DB params and test connection
    db_params = None
    if not args.download_only:
        db_params = get_db_params(args)
        try:
            conn = psycopg2.connect(**db_params)
            conn.cursor().execute("SELECT 1")
            conn.close()
            print("Database connection OK")
        except Exception as e:
            print(f"Database connection failed: {e}", file=sys.stderr)
            if args.no_download:
                sys.exit(1)
            print("Continuing with download only...\n")
            args.download_only = True
            db_params = None

    # Handle --replace: drop tables before any workers start
    if args.replace and db_params:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        for taxi_type in args.types:
            table_name = f"{taxi_type}_tripdata"
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            print(f"  Dropped table '{table_name}'")
        conn.commit()
        cur.close()
        conn.close()

    # Pre-create tables to avoid race conditions with concurrent workers
    if db_params and not args.download_only:
        ensure_tables(db_params, args.types, args.data_dir)

    # Build work items
    work_items = [
        (taxi_type, month)
        for taxi_type in args.types
        for month in months
    ]

    dl_semaphore = threading.Semaphore(args.download_workers)
    t_start = time.monotonic()

    if parallel:
        # Parallel execution
        total_rows = 0
        completed = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(process_file, taxi_type, month, args, db_params, dl_semaphore): (taxi_type, month)
                for taxi_type, month in work_items
            }
            for future in as_completed(futures):
                taxi_type, month, rows, status, elapsed = future.result()
                completed += 1
                total_rows += rows
                if rows > 0:
                    log(f"  [{taxi_type} {month}] {rows:,} rows - {elapsed:.1f}s  ({completed}/{len(work_items)})")
                elif status == "ok":
                    log(f"  [{taxi_type} {month}] downloaded - {elapsed:.1f}s  ({completed}/{len(work_items)})")

        t_elapsed = time.monotonic() - t_start
        print(f"\nCompleted {len(work_items)} files in {t_elapsed:.1f}s")
        if total_rows:
            print(f"Total rows loaded: {total_rows:,}")
    else:
        # Sequential execution (preserves verbose output)
        for taxi_type, month in work_items:
            print(f"\n[{taxi_type} {month}]")
            taxi_type, month, rows, status, elapsed = process_file(
                taxi_type, month, args, db_params, dl_semaphore
            )

    # Summary (use pg_stat estimate to avoid slow COUNT on large tables)
    if db_params and not args.download_only:
        print(f"\n{'='*60}")
        print("  SUMMARY")
        print(f"{'='*60}")
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            for taxi_type in args.types:
                table_name = f"{taxi_type}_tripdata"
                try:
                    cur.execute(
                        "SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname = %s",
                        (table_name,),
                    )
                    row = cur.fetchone()
                    if row:
                        print(f"  {table_name}: ~{row[0]:,} rows inserted")
                    else:
                        print(f"  {table_name}: table does not exist")
                except Exception:
                    conn.rollback()
                    print(f"  {table_name}: could not query stats")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"  Could not connect for summary: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
