"""DuckDB resource configuration connecting to data/weather.duckdb."""

import os
from pathlib import Path
from dagster_duckdb import DuckDBResource

# Resolve absolute path to data/weather.duckdb relative to project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "weather.duckdb"

# Allow overriding via environment variable
DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", str(DEFAULT_DB_PATH))

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# DuckDB Dagster Resource
duckdb_resource = DuckDBResource(
    database=str(DB_PATH),
)
