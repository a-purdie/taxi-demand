#!/usr/bin/env python
"""
NYC TLC Trip Record Data Ingestion

Downloads parquet files from NYC's TLC website.

Usage examples:
    # Download yellow taxi data for Jan-Mar 2024
    python ingest.py --types yellow --start 2024-01 --end 2024-03

    # Download all taxi types for 2023
    python ingest.py --start 2023-01 --end 2023-12

    # Parallel ingestion: 2 concurrent downloads
    python ingest.py --types yellow green --start 2024-01 --end 2024-12 --download-workers 2
"""

import argparse
import threading
import time
import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import requests
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
from constants import TAXI_TYPES, DATA_DIR

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SHAPE_FILE_PATH = Path("data/zones/taxi_zones.shp")

# Lock for thread-safe printing
_print_lock = threading.Lock()

# Shared rate-limit cooldown: all download threads check this before requesting
_rate_limit_lock = threading.Lock()
_rate_limit_until = 0.0  # monotonic timestamp

zones_gdf = gpd.read_file(SHAPE_FILE_PATH)
zones_gdf = zones_gdf.to_crs(epsg=4326)

def log(msg):
    with _print_lock:
        print(msg, flush=True)


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

def download_shapefile(data_dir):
    shapefile_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    shapefile_path = data_dir / "zones" / "taxi_zones.shp"
    if shapefile_path.exists():
        print("Taxi zone shapefile already exists, skipping")
        return shapefile_path
    print("Downloading taxi zone shapefile...")
    resp = requests.get(shapefile_url)
    resp.raise_for_status()
    extract_dir = data_dir / "zones"
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(extract_dir)

    print(f"Extracted to {extract_dir}")
    return shapefile_path


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
            with _rate_limit_lock:
                now = time.monotonic()
                if now >= _rate_limit_until:
                    _rate_limit_until = now + cooldown
                    log(f"  Got 403, pausing {cooldown}s before retry...")
                    cooldown *= 2
            continue

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

def assign_zones(parquet_file_path, zones_gdf):
    parquet_file = pq.ParquetFile(parquet_file_path)
    schema = parquet_file.schema_arrow
    location_in_degrees = "start_lon" in schema.names
    if not location_in_degrees:
        return
    print(f"{parquet_file_path} needs zone assignment. Assigning zones...")
    tmp_path = parquet_file_path.with_suffix(".tmp.parquet")
    writer = None

    try:
        for batch in parquet_file.iter_batches(batch_size=500000):
            batch_dataframe = batch.to_pandas()

            pickup_points = gpd.GeoDataFrame(
                batch_dataframe,
                geometry=gpd.points_from_xy(
                    batch_dataframe['start_lon'], 
                    batch_dataframe['start_lat']
                ), 
                crs="EPSG:4326"
            )

            batch_dataframe['pulocationid'] = gpd.sjoin(
                pickup_points, zones_gdf, how="left", predicate="within"
            )["LocationID"]

            dropoff_points = gpd.GeoDataFrame(
                batch_dataframe,
                geometry=gpd.points_from_xy(
                    batch_dataframe['end_lon'],
                    batch_dataframe['end_lat']
                ),
                crs="EPSG:4326"
            )

            batch_dataframe["dolocationid"] = gpd.sjoin(
                dropoff_points, zones_gdf, how="left", predicate="within"
            )["LocationID"]

            batch_dataframe = batch_dataframe.drop(columns=[
                "start_lon", "start_lat", "end_lon", "end_lat"
            ])

            table = pa.Table.from_pandas(batch_dataframe)

            if writer is None:
                writer = pq.ParquetWriter(tmp_path, table.schema)
            writer.write_table(table)
        
        if writer:
            writer.close()
            tmp_path.replace(parquet_file_path)

    except Exception:
        if writer:
            writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise

def download_one(taxi_type, month, data_dir, dl_semaphore, parallel=False, replace=False):
    """Download a single file. Returns (taxi_type, month, status, elapsed)."""
    filename = f"{taxi_type}_tripdata_{month}.parquet"
    filepath = data_dir / filename
    url = f"{BASE_URL}/{filename}"
    t0 = time.monotonic()

    if filepath.exists() and not replace:
        size_mb = filepath.stat().st_size / 1024 / 1024
        if not parallel:
            print(f"  Already downloaded ({size_mb:.1f} MB), skipping")
        elapsed = time.monotonic() - t0
        return (taxi_type, month, "exists", elapsed)

    dl_semaphore.acquire()
    try:
        if not parallel:
            print(f"  Downloading {url}")
        ok = download_file(url, filepath, quiet=parallel)
        if ok and taxi_type == "yellow":
            assign_zones(filepath, zones_gdf)
    finally:
        dl_semaphore.release()

    elapsed = time.monotonic() - t0
    if not ok:
        if parallel:
            log(f"  [{taxi_type} {month}] not found on server, skipping")
        else:
            print("  Not found on server, skipping")
        return (taxi_type, month, "not_found", elapsed)

    return (taxi_type, month, "ok", elapsed)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Download NYC TLC taxi trip data.",
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
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help=f"Directory for downloaded parquet files (default: {DATA_DIR})",
    )
    p.add_argument(
        "--download-workers",
        type=int,
        default=2,
        help="Max concurrent downloads (default: 2)",
    )

    p.add_argument(
        "--replace",
        help="Download files again even if they already exist",
        action="store_true"
    )

    return p.parse_args(argv)


def main():
    args = parse_args()
    months = list(generate_months(args.start, args.end))
    parallel = args.download_workers > 1

    print(f"Taxi types: {', '.join(args.types)}")
    print(f"Date range: {args.start} to {args.end} ({len(months)} months)")
    print(f"Total files: {len(args.types) * len(months)}")
    print(f"Download workers: {args.download_workers}")
    print(f"Replace existing files: {args.replace}")
    print()
    download_shapefile(DATA_DIR)

    # Build work items
    work_items = [
        (taxi_type, month)
        for taxi_type in args.types
        for month in months
    ]

    dl_semaphore = threading.Semaphore(args.download_workers)
    t_start = time.monotonic()

    if parallel:
        completed = 0
        with ThreadPoolExecutor(max_workers=args.download_workers) as pool:
            futures = {
                pool.submit(
                    download_one, taxi_type, month, args.data_dir, 
                    dl_semaphore, replace=args.replace, parallel=True
                ): (taxi_type, month)
                for taxi_type, month in work_items
            }
            for future in as_completed(futures):
                taxi_type, month, status, elapsed = future.result()
                completed += 1
                if status == "ok":
                    log(f"  [{taxi_type} {month}] downloaded - {elapsed:.1f}s  ({completed}/{len(work_items)})")

        t_elapsed = time.monotonic() - t_start
        print(f"\nCompleted {len(work_items)} files in {t_elapsed:.1f}s")
    else:
        for taxi_type, month in work_items:
            print(f"\n[{taxi_type} {month}]")
            download_one(taxi_type, month, args.data_dir, dl_semaphore, parallel=False)

    print("\nDone.")


if __name__ == "__main__":
    main()