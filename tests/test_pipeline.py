"""Pytest test suite testing the pipeline standalone without starting the Dagster webserver."""

import json
from unittest.mock import MagicMock, patch
import pytest

from weather_dagster.defs.client import OpenMeteoClient

# Sample realistic Open-Meteo response fixture
MOCK_WEATHER_DATA = {
    "latitude": 21.0285,
    "longitude": 105.8542,
    "timezone": "Asia/Bangkok",
    "hourly": {
        "time": [
            "2026-10-29T00:00",
            "2026-10-29T01:00",
            "2026-10-29T02:00",
            "2026-10-30T00:00",
            "2026-10-30T01:00",
        ],
        "temperature_2m": [22.5, 21.8, 21.0, 24.2, 23.5],
        "relative_humidity_2m": [85.0, 88.0, 90.0, 75.0, 78.0],
        "precipitation": [0.0, 1.5, 0.5, 0.0, 0.0],
        "wind_speed_10m": [12.0, 10.5, 9.8, 15.0, 14.2],
    },
}


def test_open_meteo_client_fetch_weather():
    """Test OpenMeteoClient correctly queries the REST API."""
    client = OpenMeteoClient(timeout=10)

    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_WEATHER_DATA
    mock_response.raise_for_status.return_value = None

    with patch.object(client.session, "get", return_value=mock_response) as mock_get:
        data = client.fetch_weather(latitude=21.0285, longitude=105.8542)

        assert data == MOCK_WEATHER_DATA
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"]["latitude"] == 21.0285
        assert call_kwargs["params"]["longitude"] == 105.8542
        assert "temperature_2m" in call_kwargs["params"]["hourly"]


def test_pipeline_definitions():
    """Test that all assets, checks, resources, and schedules are loaded in Definitions."""
    # Import Dagster modules
    from weather_dagster.definitions import defs

    asset_graph = defs.resolve_asset_graph()
    assert asset_graph is not None

    asset_keys = {key.path[-1] for key in asset_graph.get_all_asset_keys()}
    assert "open_meteo_api" in asset_keys
    assert "raw_weather" in asset_keys
    assert "cleaned_weather" in asset_keys
    assert "daily_weather_summary" in asset_keys

    # Check schedules
    schedule_names = [s.name for s in defs.schedules]
    assert "weather_daily_schedule" in schedule_names

    # Check resources
    assert "duckdb" in defs.resources


def test_full_pipeline_materialization_and_checks(tmp_path):
    """Test end-to-end execution of raw -> cleaned -> marts assets with DuckDB and asset checks."""
    from dagster import materialize
    from dagster_duckdb import DuckDBResource

    from weather_dagster.defs.assets import (
        cleaned_weather,
        daily_weather_summary,
        raw_weather,
    )
    from weather_dagster.defs.checks import (
        check_cleaned_weather_ranges,
        check_daily_summary_validity,
    )

    test_db_file = tmp_path / "test_weather.duckdb"
    test_duckdb = DuckDBResource(database=str(test_db_file))

    # Mock the OpenMeteoClient.fetch_weather call
    with patch(
        "weather_dagster.defs.assets.OpenMeteoClient.fetch_weather",
        return_value=MOCK_WEATHER_DATA,
    ):
        result = materialize(
            assets=[raw_weather, cleaned_weather, daily_weather_summary],
            resources={"duckdb": test_duckdb},
            partition_key="2026-10-29",
        )

        assert result.success

    # Verify data in DuckDB tables
    with test_duckdb.get_connection() as conn:
        raw_count = conn.execute("SELECT COUNT(*) FROM raw_weather;").fetchone()[0]
        assert raw_count == 1

        raw_row = conn.execute("SELECT source, location_name, created_at FROM raw_weather;").fetchone()
        assert raw_row[0] == "open-meteo"
        assert raw_row[1] == "Hanoi"
        assert raw_row[2] is not None

        cleaned_count = conn.execute("SELECT COUNT(*) FROM cleaned_weather;").fetchone()[0]
        assert cleaned_count == 5

        cleaned_row = conn.execute("SELECT location_name, source, created_at, updated_at FROM cleaned_weather LIMIT 1;").fetchone()
        assert cleaned_row[0] == "Hanoi"
        assert cleaned_row[1] == "open-meteo"
        assert cleaned_row[2] is not None
        assert cleaned_row[3] is not None

        summary_rows = conn.execute(
            "SELECT location_name, date, min_temperature, max_temperature, total_precipitation, record_count, created_at, updated_at FROM daily_weather_summary ORDER BY date;"
        ).fetchall()
        assert len(summary_rows) == 1

        # 2026-10-29 aggregated metrics
        day1 = summary_rows[0]
        assert day1[0] == "Hanoi"
        assert day1[2] == 21.0  # min temp
        assert day1[3] == 22.5  # max temp
        assert day1[4] == 2.0   # total precip: 0.0 + 1.5 + 0.5
        assert day1[5] == 3     # count
        assert day1[6] is not None  # created_at
        assert day1[7] is not None  # updated_at

    # Run asset checks directly against the populated test database
    cleaned_check_result = check_cleaned_weather_ranges(duckdb=test_duckdb)
    assert cleaned_check_result.passed is True

    summary_check_result = check_daily_summary_validity(duckdb=test_duckdb)
    assert summary_check_result.passed is True
