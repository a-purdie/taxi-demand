# taxi-demand

This project is a work in progress. The plan is to forecast ride demand in NYC using public data the city publishes on its website every month. A full CI/CD implementation that automatically ingests new data as it's published and retrains accordingly, all deployed on Google Cloud Platform, is the end goal.

## What's implemented
- A data ingestion script that downloads the `.parquet` files from the TLC Trip Record Data page and uploads those files to a local Postgres instance. It's set up this way just for now for local model development, but eventually this will be modified to use GCP's BigQuery.
- A DuckDB-based data loading utility that creates normalized views over the raw parquet files for exploratory analysis.

## What's coming next
- A notebook with a model trained on a small slice of the dataset (it's huge and messy). Will probably test out a few different methodologies and compare.

---

## Module documentation

### `ingest.py` — Data ingestion and schema normalization

This script handles downloading NYC TLC taxi trip parquet files from the city's CDN and (optionally) loading them into a PostgreSQL database. It also serves as the **single source of truth** for canonical column schemas and rename mappings, which other modules import from.

#### What it does

1. **Downloads parquet files** from `https://d37ci6vzurychx.cloudfront.net/trip-data` for any combination of taxi types (`yellow`, `green`, `fhv`, `fhvhv`) and month ranges.
2. **Normalizes Arrow tables** to a canonical schema per taxi type — renaming columns, casting types, and null-filling columns that don't exist in older files.
3. **Loads into PostgreSQL** via `COPY FROM STDIN` with CSV serialization, batched in configurable chunks (default 100k rows).

#### Schema normalization

The NYC TLC data has gone through multiple schema changes over the years. The yellow taxi dataset is the most complex case, with three eras of column naming:

| Era | Example columns | Notes |
|---|---|---|
| 2009 (original) | `vendor_name`, `Trip_Pickup_DateTime`, `Start_Lon` | PascalCase, `vendor_name` is VARCHAR |
| 2010–2014 | `vendor_id`, `pickup_datetime`, `pickup_longitude` | Renamed, still has lat/lon |
| 2015+ (modern) | `VendorID`, `tpep_pickup_datetime`, `PULocationID` | Zone IDs replace lat/lon |

The convention is to use the **oldest** column name as the canonical name (e.g., `vendor_name` not `VendorID`, `fare_amt` not `fare_amount`). The `YELLOW_RENAMES` dictionary maps all later names back to these originals.

Green, FHV, and FHVHV schemas are relatively stable across eras and only need type casting and null-filling for columns added in later years (e.g., `cbd_congestion_fee` added in 2025).

#### Key exports (used by other modules)

| Export | Type | Description |
|---|---|---|
| `TAXI_TYPES` | `list[str]` | `["yellow", "green", "fhv", "fhvhv"]` |
| `DATA_DIR` | `Path` | Default data directory (`./data`) |
| `CANONICAL_SCHEMAS` | `dict` | Maps taxi type → list of `(column_name, pg_type, arrow_type)` tuples |
| `YELLOW_RENAMES` | `dict` | Maps raw parquet column names (lowercased) → canonical names |
| `YELLOW_DROP` | `set` | Column names to discard (e.g., `__index_level_0__`) |
| `NORMALIZERS` | `dict` | Maps taxi type → normalizer function (`Arrow table → Arrow table`) |

#### Rate limiting

The NYC TLC CDN rate-limits after ~20–30 downloads. The script handles this with:
- A shared monotonic cooldown timer (`_rate_limit_until`) across all download threads.
- On a 403 response, a 30-second cooldown is set. All threads wait before retrying.
- A second 403 after the cooldown is treated as "file doesn't exist" (not rate limiting).
- A semaphore (`--download-workers`, default 2) caps concurrent downloads.

#### CLI usage

```bash
# Download all taxi types for a date range (no DB load)
python ingest.py --start 2023-01 --end 2023-12 --download-only

# Download yellow and green, load into PostgreSQL
python ingest.py --types yellow green --start 2024-01 --end 2024-06

# Load already-downloaded files into PostgreSQL (skip download)
python ingest.py --types yellow --start 2024-01 --end 2024-03 --no-download

# Drop and recreate tables before loading
python ingest.py --types yellow --start 2024-01 --end 2024-12 --replace

# Tune parallelism: 2 concurrent downloads, 8 DB load workers
python ingest.py --types yellow --start 2020-01 --end 2024-12 --download-workers 2 --workers 8
```

#### CLI flags

| Flag | Default | Description |
|---|---|---|
| `--types` | all four | Which taxi types to process |
| `--start` | *(required)* | Start month (`YYYY-MM`) |
| `--end` | *(required)* | End month (`YYYY-MM`) |
| `--download-only` | off | Download parquet files without loading to DB |
| `--no-download` | off | Skip download, load existing local files |
| `--replace` | off | Drop and recreate tables before loading |
| `--data-dir` | `./data` | Directory for parquet files |
| `--batch-size` | 100,000 | Rows per `COPY` batch |
| `--workers` | 4 | Parallel DB load threads |
| `--download-workers` | 2 | Max concurrent downloads |
| `--db-host` | `$POSTGRES_HOST` or `localhost` | PostgreSQL host |
| `--db-port` | `$POSTGRES_PORT` or `5432` | PostgreSQL port |
| `--db-name` | `$POSTGRES_DB` or `nyc_taxi` | Database name |
| `--db-user` | `$POSTGRES_USER` or `postgres` | Database user |
| `--db-password` | `$POSTGRES_PASSWORD` or empty | Database password |

#### Execution flow

1. Parse CLI arguments and generate the list of `(taxi_type, month)` work items.
2. Test the PostgreSQL connection (falls back to download-only if it fails).
3. If `--replace`, drop the target tables.
4. Pre-create tables using canonical schemas to avoid race conditions with parallel workers.
5. For each work item (`process_file`):
   - Download the parquet file if not already present (gated by the download semaphore).
   - Read the file in batches via PyArrow, normalize each batch through the appropriate normalizer, serialize to CSV, and `COPY` into PostgreSQL.
6. Print a summary of rows inserted using `pg_stat_user_tables`.

When `--workers` > 1, work items run in a `ThreadPoolExecutor`; otherwise they run sequentially with verbose per-file output.

---

### `utils/eda_data_loading.py` — DuckDB views for exploratory analysis

This module creates DuckDB views over the raw parquet files that normalize column names at query time. It's the primary interface for querying the dataset in notebooks and scripts — no data is copied or loaded into memory until a query is actually executed, and DuckDB only reads the columns and row groups it needs.

#### What it does

1. Imports canonical schemas and rename mappings from `ingest.py` (single source of truth).
2. Scans the parquet files to discover which columns actually exist across all files for a taxi type.
3. Builds `CREATE VIEW` statements with `CAST` and `COALESCE` expressions to present a unified schema.
4. Returns a DuckDB connection with all views ready to query.

#### How the views are built

**Yellow taxi** (`_build_yellow_view`) — the complex case:
- Reads the union schema from all `yellow_tripdata_*.parquet` files using `union_by_name=true`.
- For each canonical column, collects all known aliases from `YELLOW_RENAMES` (e.g., `fare_amt` has alias `fare_amount`).
- Checks which aliases actually exist in the parquet files.
- If one source column exists: `CAST(col AS type) AS canonical_name`.
- If multiple exist (across different eras): `COALESCE(CAST(col1 AS type), CAST(col2 AS type)) AS canonical_name` — whichever file has a non-null value wins.
- If none exist: `NULL::type AS canonical_name`.

**Green, FHV, FHVHV** (`_build_simple_view`) — these have stable schemas, so the view just casts types and null-fills any missing columns without needing COALESCE across aliases.

Every view includes a `source_file` column (from DuckDB's `filename=true`) containing the full path to the originating parquet file. This is useful for filtering by month: `WHERE source_file LIKE '%2024-01%'`.

#### Views created

| View name | Taxi type | Columns |
|---|---|---|
| `yellow_trips` | Yellow | 22 canonical + `source_file` |
| `green_trips` | Green | 21 canonical + `source_file` |
| `fhv_trips` | FHV | 7 canonical + `source_file` |
| `fhvhv_trips` | FHVHV | 24 canonical + `source_file` |

#### API reference

**`get_connection(data_dir=DATA_DIR)`**

Returns a DuckDB connection with all views created. This is the main entry point.

```python
from utils.eda_data_loading import get_connection

con = get_connection()

# Query yellow taxi data with canonical column names
con.sql("SELECT avg(fare_amt), avg(tip_amt) FROM yellow_trips").show()

# Filter to a specific month
con.sql("""
    SELECT count(*) FROM yellow_trips
    WHERE source_file LIKE '%2024-01%'
""").show()

# Convert to pandas (careful with large result sets on a 64GB machine)
df = con.sql("SELECT * FROM green_trips WHERE source_file LIKE '%2024-06%'").df()
```

**`create_views(con, data_dir=DATA_DIR)`**

Creates normalized views on an existing DuckDB connection. Called internally by `get_connection`, but available if you want to add views to a connection you've already configured (e.g., one with custom memory limits).

```python
import duckdb
from utils.eda_data_loading import create_views

con = duckdb.connect()
con.sql("SET memory_limit = '50GB'")
create_views(con)
```

**`list_available_files(taxi_type, data_dir=DATA_DIR)`**

Returns a list of `(month_str, filepath)` tuples for the given taxi type, sorted by date. Useful for checking what data is on disk.

```python
from utils.eda_data_loading import list_available_files

files = list_available_files("yellow")
# [("2009-01", PosixPath(".../yellow_tripdata_2009-01.parquet")), ...]
print(f"{len(files)} files, from {files[0][0]} to {files[-1][0]}")
```

**`summarize_available_data(con)`**

Prints row counts and date ranges for all taxi types. Scans the parquet metadata, so it's fast.

```python
from utils.eda_data_loading import get_connection, summarize_available_data

con = get_connection()
summarize_available_data(con)
# yellow:  1,847,259,126 rows (2009-01 to 2025-01)
# green:    103,042,841 rows (2013-08 to 2025-01)
#   fhv:    832,951,753 rows (2015-01 to 2024-06)
# fhvhv:  1,247,654,032 rows (2019-02 to 2025-01)
```

**`VIEW_NAMES`**

A dict mapping taxi type to view name: `{"yellow": "yellow_trips", "green": "green_trips", ...}`.

#### Tips for working with this module

- **Set a memory limit** when querying large slices of the data. The VM has 64GB of RAM and the full dataset is ~71GB of parquet files. DuckDB will try to use as much memory as it can, so `con.sql("SET memory_limit = '50GB'")` is a good safeguard.
- **Use approximate quantiles** (`approx_quantile`) instead of exact ones (`quantile_cont`, `median`) on large datasets — exact quantiles require sorting the entire column in memory and can OOM.
- **Compute histograms server-side** in DuckDB (e.g., `SELECT floor(col / bin_width) * bin_width AS bin, count(*)`) and only pull aggregated counts into Python.
- **Filter by `source_file`** to work with a subset of months without scanning the entire dataset.
