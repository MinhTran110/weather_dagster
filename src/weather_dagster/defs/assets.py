"""Dagster 3-tier assets: raw -> cleaned -> marts for weather data."""

import json
from datetime import datetime, timezone
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dagster_duckdb import DuckDBResource

from weather_dagster.defs.client import OpenMeteoClient


@asset(
    group_name="raw",
    description="Raw JSON forecast data fetched from Open-Meteo REST API and stored in DuckDB.",
)
def raw_weather(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Fetch raw weather data from Open-Meteo API and store in DuckDB raw_weather table."""
    client = OpenMeteoClient()
    context.log.info("Fetching weather data from Open-Meteo API...")
    payload = client.fetch_weather()

    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_weather (
                ingested_at TIMESTAMP,
                payload VARCHAR
            );
            """
        )
        conn.execute(
            "INSERT INTO raw_weather VALUES (?, ?);",
            [datetime.now(timezone.utc), json.dumps(payload)],
        )
        total_batches = conn.execute("SELECT COUNT(*) FROM raw_weather;").fetchone()[0]

    context.log.info(f"Raw weather data saved. Total raw batches: {total_batches}")
    return MaterializeResult(
        metadata={
            "total_raw_batches": MetadataValue.int(total_batches),
            "latitude": MetadataValue.float(payload.get("latitude", 0.0)),
            "longitude": MetadataValue.float(payload.get("longitude", 0.0)),
            "timezone": MetadataValue.text(payload.get("timezone", "UTC")),
        }
    )


@asset(
    deps=[raw_weather],
    group_name="cleaned",
    description="Cleaned, unnested hourly weather metrics with typed columns and deduplication.",
)
def cleaned_weather(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Parse and clean raw weather JSON into tabular cleaned_weather table."""
    with duckdb.get_connection() as conn:
        # Retrieve the latest raw payload
        row = conn.execute(
            "SELECT payload FROM raw_weather ORDER BY ingested_at DESC LIMIT 1;"
        ).fetchone()

        if not row:
            raise ValueError("No raw weather data found in raw_weather table.")

        raw_data = json.loads(row[0])
        hourly = raw_data.get("hourly", {})

        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidities = hourly.get("relative_humidity_2m", [])
        precipitations = hourly.get("precipitation", [])
        wind_speeds = hourly.get("wind_speed_10m", [])

        # Create cleaned table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cleaned_weather (
                timestamp TIMESTAMP PRIMARY KEY,
                temperature_2m DOUBLE,
                relative_humidity_2m DOUBLE,
                precipitation DOUBLE,
                wind_speed_10m DOUBLE
            );
            """
        )

        # Transform and clean records
        cleaned_records = []
        for i in range(len(times)):
            t_str = times[i]
            if not t_str:
                continue
            # Parse ISO timestamp
            t_val = datetime.fromisoformat(t_str)
            temp = float(temperatures[i]) if i < len(temperatures) and temperatures[i] is not None else None
            hum = float(humidities[i]) if i < len(humidities) and humidities[i] is not None else None
            precip = float(precipitations[i]) if i < len(precipitations) and precipitations[i] is not None else 0.0
            wind = float(wind_speeds[i]) if i < len(wind_speeds) and wind_speeds[i] is not None else None

            cleaned_records.append((t_val, temp, hum, precip, wind))

        # Upsert cleaned records
        conn.executemany(
            """
            INSERT OR REPLACE INTO cleaned_weather (
                timestamp, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
            ) VALUES (?, ?, ?, ?, ?);
            """,
            cleaned_records,
        )

        total_cleaned_rows = conn.execute("SELECT COUNT(*) FROM cleaned_weather;").fetchone()[0]

    context.log.info(f"Cleaned weather records updated. Total records in DB: {total_cleaned_rows}")
    return MaterializeResult(
        metadata={
            "processed_records": MetadataValue.int(len(cleaned_records)),
            "total_records": MetadataValue.int(total_cleaned_rows),
        }
    )


@asset(
    deps=[cleaned_weather],
    group_name="marts",
    description="Aggregated daily weather mart with min/max/avg temperature, precipitation, and wind.",
)
def daily_weather_summary(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Aggregate cleaned weather data into daily summary marts table."""
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_weather_summary (
                date DATE PRIMARY KEY,
                avg_temperature DOUBLE,
                min_temperature DOUBLE,
                max_temperature DOUBLE,
                total_precipitation DOUBLE,
                avg_wind_speed DOUBLE,
                record_count BIGINT
            );
            """
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO daily_weather_summary
            SELECT
                CAST(timestamp AS DATE) AS date,
                ROUND(AVG(temperature_2m), 2) AS avg_temperature,
                ROUND(MIN(temperature_2m), 2) AS min_temperature,
                ROUND(MAX(temperature_2m), 2) AS max_temperature,
                ROUND(SUM(precipitation), 2) AS total_precipitation,
                ROUND(AVG(wind_speed_10m), 2) AS avg_wind_speed,
                COUNT(*) AS record_count
            FROM cleaned_weather
            GROUP BY CAST(timestamp AS DATE)
            ORDER BY date;
            """
        )

        total_days = conn.execute("SELECT COUNT(*) FROM daily_weather_summary;").fetchone()[0]

    context.log.info(f"Daily weather mart updated. Total summary days: {total_days}")
    return MaterializeResult(
        metadata={
            "total_days_summarized": MetadataValue.int(total_days),
        }
    )
