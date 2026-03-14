"""
DuckDB-based utility for querying NYC TLC taxi trip data with consistent column names.

Creates views over raw parquet files that normalize column names across all eras,
so you can query any date range with a single consistent schema. No data is loaded
into memory — DuckDB reads parquet files directly.

Usage:
    from utils.data_loading import get_connection

    con = get_connection()

    # Query with canonical column names regardless of file era
    con.sql("SELECT avg(fare_amt), avg(tip_amt) FROM yellow_trips").show()

    # Filter by month using the source_file column
    con.sql(\"\"\"
        SELECT count(*) FROM yellow_trips
        WHERE source_file LIKE '%2024-01%'
    \"\"\").show()
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow as pa

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from ingest import (
    CANONICAL_SCHEMAS,
    DATA_DIR,
    TAXI_TYPES,
    YELLOW_DROP,
    YELLOW_RENAMES,
)

# Map Arrow types from ingest.py schemas to DuckDB SQL cast types
_ARROW_TO_DUCKDB = {
    pa.string(): "VARCHAR",
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
}

_MONTH_RE = re.compile(r"(\d{4}-\d{2})")

# View names for each taxi type
VIEW_NAMES = {t: f"{t}_trips" for t in TAXI_TYPES}


def list_available_files(taxi_type, data_dir=DATA_DIR):
    """
    List available parquet files for a taxi type, sorted by date.

    Returns:
        List of (month_str, filepath) tuples.
    """
    if taxi_type not in TAXI_TYPES:
        raise ValueError(f"Unknown taxi type '{taxi_type}'. Must be one of: {TAXI_TYPES}")

    files = sorted(Path(data_dir).glob(f"{taxi_type}_tripdata_*.parquet"))
    result = []
    for f in files:
        match = _MONTH_RE.search(f.stem)
        if match:
            result.append((match.group(1), f))
    return result


def _get_union_columns(con, glob_pattern):
    """Get the set of column names from a union_by_name read (lowercased)."""
    result = con.sql(
        f"SELECT * FROM read_parquet('{glob_pattern}', "
        f"union_by_name=true, filename=true) LIMIT 0"
    )
    return {col.lower(): col for col in result.columns}


def _build_yellow_view(con, data_dir):
    """Build the yellow taxi view with COALESCE for renamed columns."""
    glob = str(Path(data_dir) / "yellow_tripdata_*.parquet")
    available = _get_union_columns(con, glob)

    # Invert YELLOW_RENAMES: canonical_name -> [alias1, alias2, ...]
    alias_groups = defaultdict(list)
    for raw, canonical in YELLOW_RENAMES.items():
        alias_groups[canonical].append(raw)

    selects = []
    for col_name, _, arrow_type in CANONICAL_SCHEMAS["yellow"]:
        # All candidates: the canonical name + any aliases
        candidates = [col_name] + alias_groups.get(col_name, [])
        # Keep only those that actually exist in the parquet files
        existing = [available[c.lower()] for c in candidates if c.lower() in available]
        duckdb_type = _ARROW_TO_DUCKDB.get(arrow_type, "VARCHAR")

        if not existing:
            selects.append(f"NULL::{duckdb_type} AS {col_name}")
        elif len(existing) == 1:
            selects.append(f"CAST({existing[0]} AS {duckdb_type}) AS {col_name}")
        else:
            casted = ", ".join(f"CAST({c} AS {duckdb_type})" for c in existing)
            selects.append(f"COALESCE({casted}) AS {col_name}")

    select_clause = ",\n    ".join(selects)
    con.sql(f"""
        CREATE OR REPLACE VIEW yellow_trips AS
        SELECT
            {select_clause},
            filename AS source_file
        FROM read_parquet('{glob}', union_by_name=true, filename=true)
    """)


def _build_simple_view(con, taxi_type, data_dir):
    """Build a view for taxi types without column renames (green, fhv, fhvhv)."""
    glob = str(Path(data_dir) / f"{taxi_type}_tripdata_*.parquet")
    available = _get_union_columns(con, glob)

    selects = []
    for col_name, _, arrow_type in CANONICAL_SCHEMAS[taxi_type]:
        duckdb_type = _ARROW_TO_DUCKDB.get(arrow_type, "VARCHAR")
        if col_name.lower() in available:
            actual = available[col_name.lower()]
            selects.append(f"CAST({actual} AS {duckdb_type}) AS {col_name}")
        else:
            selects.append(f"NULL::{duckdb_type} AS {col_name}")

    select_clause = ",\n    ".join(selects)
    view_name = VIEW_NAMES[taxi_type]
    con.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
            {select_clause},
            filename AS source_file
        FROM read_parquet('{glob}', union_by_name=true, filename=true)
    """)


def create_views(con, data_dir=DATA_DIR):
    """Create normalized views for all taxi types that have data files."""
    data_dir = Path(data_dir)
    for taxi_type in TAXI_TYPES:
        files = list(data_dir.glob(f"{taxi_type}_tripdata_*.parquet"))
        if not files:
            continue
        if taxi_type == "yellow":
            _build_yellow_view(con, data_dir)
        else:
            _build_simple_view(con, taxi_type, data_dir)


def get_connection(data_dir=DATA_DIR):
    """
    Return a DuckDB connection with normalized views for all taxi types.

    Views created:
        yellow_trips  — canonical yellow taxi schema (24 columns + source_file)
        green_trips   — canonical green taxi schema (21 columns + source_file)
        fhv_trips     — canonical FHV schema (7 columns + source_file)
        fhvhv_trips   — canonical FHVHV schema (24 columns + source_file)

    All views read parquet files directly — no data is loaded into memory until
    you run a query, and DuckDB only reads the columns/row groups it needs.
    """
    con = duckdb.connect()
    create_views(con, data_dir)
    return con


def summarize_available_data(con):
    """Print a summary of row counts and date ranges per taxi type."""
    for taxi_type in TAXI_TYPES:
        view = VIEW_NAMES[taxi_type]
        try:
            result = con.sql(f"""
                SELECT
                    count(*) AS total_rows,
                    min(source_file) AS first_file,
                    max(source_file) AS last_file
                FROM {view}
            """).fetchone()
            total_rows, first_file, last_file = result
            first_month = _MONTH_RE.search(first_file)
            last_month = _MONTH_RE.search(last_file)
            first = first_month.group(1) if first_month else "?"
            last = last_month.group(1) if last_month else "?"
            print(f"{taxi_type:>5}: {total_rows:>14,} rows ({first} to {last})")
        except duckdb.CatalogException:
            print(f"{taxi_type:>5}: no data found")
