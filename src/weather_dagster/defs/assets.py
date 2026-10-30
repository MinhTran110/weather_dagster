"""Dagster 3-tier assets with daily partitioning, visual lineage, and table schemas."""

import json
import uuid
from datetime import datetime, timezone
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    TableColumn,
    TableSchema,
    asset,
)
from dagster_duckdb import DuckDBResource

from weather_dagster.defs.client import OpenMeteoClient


DEFAULT_LOCATION = "Hanoi"
DEFAULT_LAT = 21.0285
DEFAULT_LON = 105.8542
DATA_SOURCE = "open-meteo"
DB_FILE_PATH = "data/weather.duckdb"

# 1. External Source Asset representing Open-Meteo REST API
open_meteo_api = AssetSpec(
    key="open_meteo_api",
    description="Open-Meteo Global Weather REST API (https://api.open-meteo.com/v1/forecast).",
    kinds={"api", "rest"},
    group_name="sources",
    metadata={
        "endpoint": MetadataValue.url("https://api.open-meteo.com/v1/forecast"),
        "provider": MetadataValue.text("Open-Meteo"),
        "auth_type": MetadataValue.text("Public (No API Key Required)"),
        "rate_limit": MetadataValue.text("10,000 calls / day"),
    },
)

# 2. Database Table Schemas for Rich Display on Dagster UI
RAW_WEATHER_SCHEMA = TableSchema(
    columns=[
        TableColumn("raw_id", "VARCHAR", description="UUID định danh duy nhất cho từng batch cào"),
        TableColumn("source", "VARCHAR", description="Nguồn dữ liệu gốc (open-meteo)"),
        TableColumn("location_name", "VARCHAR", description="Tên địa điểm (Hanoi)"),
        TableColumn("partition_date", "VARCHAR", description="Ngày phân vùng (YYYY-MM-DD)"),
        TableColumn("latitude", "DOUBLE", description="Vĩ độ"),
        TableColumn("longitude", "DOUBLE", description="Kinh độ"),
        TableColumn("dagster_run_id", "VARCHAR", description="Mã Run ID của Dagster thực thi"),
        TableColumn("created_at", "TIMESTAMP", description="Thời điểm ghi nhận dữ liệu vào DB (UTC)"),
        TableColumn("payload", "VARCHAR", description="Chuỗi JSON thô nguyên bản từ Open-Meteo API"),
    ]
)

CLEANED_WEATHER_SCHEMA = TableSchema(
    columns=[
        TableColumn("location_name", "VARCHAR", description="Tên địa điểm (Khóa chính kết hợp)"),
        TableColumn("timestamp", "TIMESTAMP", description="Mốc thời gian đo theo giờ (Khóa chính kết hợp)"),
        TableColumn("temperature_2m", "DOUBLE", description="Nhiệt độ không khí ở độ cao 2m (°C)"),
        TableColumn("relative_humidity_2m", "DOUBLE", description="Độ ẩm không khí tương đối (%)"),
        TableColumn("precipitation", "DOUBLE", description="Lượng mưa tích lũy trong giờ (mm)"),
        TableColumn("wind_speed_10m", "DOUBLE", description="Tốc độ gió ở độ cao 10m (km/h)"),
        TableColumn("source", "VARCHAR", description="Nguồn dữ liệu gốc (open-meteo)"),
        TableColumn("latitude", "DOUBLE", description="Vĩ độ"),
        TableColumn("longitude", "DOUBLE", description="Kinh độ"),
        TableColumn("dagster_run_id", "VARCHAR", description="Mã Run ID cập nhật gần nhất"),
        TableColumn("created_at", "TIMESTAMP", description="Thời điểm bản ghi được tạo lần đầu"),
        TableColumn("updated_at", "TIMESTAMP", description="Thời điểm bản ghi được cập nhật mới nhất"),
    ]
)

DAILY_SUMMARY_SCHEMA = TableSchema(
    columns=[
        TableColumn("location_name", "VARCHAR", description="Tên địa điểm (Khóa chính kết hợp)"),
        TableColumn("date", "DATE", description="Ngày tổng hợp (Khóa chính kết hợp)"),
        TableColumn("avg_temperature", "DOUBLE", description="Nhiệt độ trung bình trong ngày (°C)"),
        TableColumn("min_temperature", "DOUBLE", description="Nhiệt độ thấp nhất trong ngày (°C)"),
        TableColumn("max_temperature", "DOUBLE", description="Nhiệt độ cao nhất trong ngày (°C)"),
        TableColumn("total_precipitation", "DOUBLE", description="Tổng lượng mưa tích lũy cả ngày (mm)"),
        TableColumn("avg_wind_speed", "DOUBLE", description="Tốc độ gió trung bình cả ngày (km/h)"),
        TableColumn("record_count", "BIGINT", description="Số giờ đo được tổng hợp (thường là 24 giờ)"),
        TableColumn("dagster_run_id", "VARCHAR", description="Mã Run ID cập nhật gần nhất"),
        TableColumn("created_at", "TIMESTAMP", description="Thời điểm bản ghi được tạo lần đầu"),
        TableColumn("updated_at", "TIMESTAMP", description="Thời điểm bản ghi được cập nhật mới nhất"),
    ]
)

# Daily partition definition starting from 2026-10-01, running at 06:00 AM Asia/Ho_Chi_Minh
daily_partitions = DailyPartitionsDefinition(
    start_date="2026-10-01",
    hour_offset=6,
    end_offset=1,
    timezone="Asia/Ho_Chi_Minh",
)


def _get_partition_date(context: AssetExecutionContext) -> str:
    """Helper to safely retrieve partition key or fallback to today's UTC date."""
    if context.has_partition_key:
        return context.partition_key
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@asset(
    deps=[open_meteo_api],
    partitions_def=daily_partitions,
    kinds={"duckdb", "python"},
    group_name="raw",
    description="Raw JSON weather forecast or historical data partitioned by date from Open-Meteo into DuckDB.",
)
def raw_weather(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Fetch raw weather data for the partition date and store in DuckDB raw_weather table."""
    partition_date = _get_partition_date(context)
    client = OpenMeteoClient()
    context.log.info(f"Fetching weather data for {DEFAULT_LOCATION} on date {partition_date}...")

    # Fetch data specifically for this partition date (24 hours)
    payload = client.fetch_weather(
        latitude=DEFAULT_LAT,
        longitude=DEFAULT_LON,
        start_date=partition_date,
        end_date=partition_date,
    )

    now = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())

    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_weather (
                raw_id VARCHAR PRIMARY KEY,
                source VARCHAR,
                location_name VARCHAR,
                partition_date VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                dagster_run_id VARCHAR,
                created_at TIMESTAMP,
                payload VARCHAR
            );
            ALTER TABLE raw_weather ADD COLUMN IF NOT EXISTS partition_date VARCHAR;
            """
        )
        conn.execute(
            """
            INSERT INTO raw_weather (
                raw_id, source, location_name, partition_date, latitude, longitude, dagster_run_id, created_at, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                batch_id,
                DATA_SOURCE,
                DEFAULT_LOCATION,
                partition_date,
                payload.get("latitude", DEFAULT_LAT),
                payload.get("longitude", DEFAULT_LON),
                context.run.run_id,
                now,
                json.dumps(payload),
            ],
        )
        total_batches = conn.execute("SELECT COUNT(*) FROM raw_weather;").fetchone()[0]

    context.log.info(f"Raw weather batch {batch_id} saved for {partition_date}. Total batches: {total_batches}")
    return MaterializeResult(
        metadata={
            "dagster/table_name": MetadataValue.text("raw_weather"),
            "dagster/column_schema": MetadataValue.table_schema(RAW_WEATHER_SCHEMA),
            "database_file": MetadataValue.path(DB_FILE_PATH),
            "raw_id": MetadataValue.text(batch_id),
            "partition_date": MetadataValue.text(partition_date),
            "source": MetadataValue.text(DATA_SOURCE),
            "location_name": MetadataValue.text(DEFAULT_LOCATION),
            "total_raw_batches": MetadataValue.int(total_batches),
            "dagster_run_id": MetadataValue.text(context.run.run_id or "manual"),
        }
    )


@asset(
    deps=[raw_weather],
    partitions_def=daily_partitions,
    kinds={"duckdb", "sql"},
    group_name="cleaned",
    description="Cleaned, unnested hourly weather metrics partitioned by date with audit timestamps in DuckDB.",
)
def cleaned_weather(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Parse and clean raw weather JSON into tabular cleaned_weather for the partition date."""
    partition_date = _get_partition_date(context)
    now = datetime.now(timezone.utc)

    with duckdb.get_connection() as conn:
        # Retrieve the latest raw payload for this partition date
        row = conn.execute(
            """
            SELECT location_name, source, latitude, longitude, payload
            FROM raw_weather
            WHERE partition_date = ?
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            [partition_date],
        ).fetchone()

        # Fallback if no specific partition row found
        if not row:
            row = conn.execute(
                """
                SELECT location_name, source, latitude, longitude, payload
                FROM raw_weather
                ORDER BY created_at DESC
                LIMIT 1;
                """
            ).fetchone()

        if not row:
            raise ValueError(f"No raw weather data found in raw_weather table for {partition_date}.")

        loc_name, src, lat, lon, payload_str = row
        raw_data = json.loads(payload_str)
        hourly = raw_data.get("hourly", {})

        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidities = hourly.get("relative_humidity_2m", [])
        precipitations = hourly.get("precipitation", [])
        wind_speeds = hourly.get("wind_speed_10m", [])

        # Create cleaned table with composite primary key: (location_name, timestamp)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cleaned_weather (
                location_name VARCHAR,
                timestamp TIMESTAMP,
                temperature_2m DOUBLE,
                relative_humidity_2m DOUBLE,
                precipitation DOUBLE,
                wind_speed_10m DOUBLE,
                source VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                dagster_run_id VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (location_name, timestamp)
            );
            """
        )

        cleaned_records = []
        for i in range(len(times)):
            t_str = times[i]
            if not t_str:
                continue
            t_val = datetime.fromisoformat(t_str)
            temp = float(temperatures[i]) if i < len(temperatures) and temperatures[i] is not None else None
            hum = float(humidities[i]) if i < len(humidities) and humidities[i] is not None else None
            precip = float(precipitations[i]) if i < len(precipitations) and precipitations[i] is not None else 0.0
            wind = float(wind_speeds[i]) if i < len(wind_speeds) and wind_speeds[i] is not None else None

            cleaned_records.append((
                loc_name,
                t_val,
                temp,
                hum,
                precip,
                wind,
                src,
                lat,
                lon,
                context.run.run_id,
                now,  # created_at
                now,  # updated_at
            ))

        # Upsert with ON CONFLICT preserving original created_at
        conn.executemany(
            """
            INSERT INTO cleaned_weather (
                location_name, timestamp, temperature_2m, relative_humidity_2m, precipitation,
                wind_speed_10m, source, latitude, longitude, dagster_run_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (location_name, timestamp) DO UPDATE SET
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                precipitation = EXCLUDED.precipitation,
                wind_speed_10m = EXCLUDED.wind_speed_10m,
                dagster_run_id = EXCLUDED.dagster_run_id,
                updated_at = EXCLUDED.updated_at;
            """,
            cleaned_records,
        )

        total_cleaned_rows = conn.execute("SELECT COUNT(*) FROM cleaned_weather;").fetchone()[0]

        # Sample for Markdown preview table on Dagster UI
        sample_cleaned = conn.execute(
            """
            SELECT timestamp, temperature_2m, relative_humidity_2m, precipitation, wind_speed_10m
            FROM cleaned_weather
            WHERE CAST(timestamp AS DATE) = CAST(? AS DATE)
            ORDER BY timestamp
            LIMIT 5;
            """,
            [partition_date],
        ).fetchall()

        md_cleaned = [
            f"### Dữ liệu mẫu ngày {partition_date} ({loc_name})",
            "| Giờ (Timestamp) | Nhiệt độ (°C) | Độ ẩm (%) | Lượng mưa (mm) | Tốc độ gió (km/h) |",
            "|:---:|:---:|:---:|:---:|:---:|",
        ]
        for r in sample_cleaned:
            md_cleaned.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} |")
        cleaned_preview_md = "\n".join(md_cleaned)

    context.log.info(f"Cleaned weather updated for {loc_name} on {partition_date}. Total records in DB: {total_cleaned_rows}")
    return MaterializeResult(
        metadata={
            "dagster/table_name": MetadataValue.text("cleaned_weather"),
            "dagster/column_schema": MetadataValue.table_schema(CLEANED_WEATHER_SCHEMA),
            "database_file": MetadataValue.path(DB_FILE_PATH),
            "partition_date": MetadataValue.text(partition_date),
            "location_name": MetadataValue.text(loc_name),
            "processed_records": MetadataValue.int(len(cleaned_records)),
            "total_records": MetadataValue.int(total_cleaned_rows),
            "dagster_run_id": MetadataValue.text(context.run.run_id or "manual"),
            "hourly_preview": MetadataValue.md(cleaned_preview_md),
        }
    )


@asset(
    deps=[cleaned_weather],
    partitions_def=daily_partitions,
    kinds={"duckdb", "sql"},
    group_name="marts",
    description="Aggregated daily weather mart partitioned by date with audit timestamps and lineage in DuckDB.",
)
def daily_weather_summary(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Aggregate cleaned weather data into daily summary marts table for the partition date."""
    partition_date = _get_partition_date(context)
    now = datetime.now(timezone.utc)

    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_weather_summary (
                location_name VARCHAR,
                date DATE,
                avg_temperature DOUBLE,
                min_temperature DOUBLE,
                max_temperature DOUBLE,
                total_precipitation DOUBLE,
                avg_wind_speed DOUBLE,
                record_count BIGINT,
                dagster_run_id VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (location_name, date)
            );
            """
        )

        conn.execute(
            """
            INSERT INTO daily_weather_summary
            SELECT
                location_name,
                CAST(timestamp AS DATE) AS date,
                ROUND(AVG(temperature_2m), 2) AS avg_temperature,
                ROUND(MIN(temperature_2m), 2) AS min_temperature,
                ROUND(MAX(temperature_2m), 2) AS max_temperature,
                ROUND(SUM(precipitation), 2) AS total_precipitation,
                ROUND(AVG(wind_speed_10m), 2) AS avg_wind_speed,
                COUNT(*) AS record_count,
                ? AS dagster_run_id,
                ? AS created_at,
                ? AS updated_at
            FROM cleaned_weather
            WHERE CAST(timestamp AS DATE) = CAST(? AS DATE)
            GROUP BY location_name, CAST(timestamp AS DATE)
            ON CONFLICT (location_name, date) DO UPDATE SET
                avg_temperature = EXCLUDED.avg_temperature,
                min_temperature = EXCLUDED.min_temperature,
                max_temperature = EXCLUDED.max_temperature,
                total_precipitation = EXCLUDED.total_precipitation,
                avg_wind_speed = EXCLUDED.avg_wind_speed,
                record_count = EXCLUDED.record_count,
                dagster_run_id = EXCLUDED.dagster_run_id,
                updated_at = EXCLUDED.updated_at;
            """,
            [context.run.run_id, now, now, partition_date],
        )

        total_days = conn.execute("SELECT COUNT(*) FROM daily_weather_summary;").fetchone()[0]

        summary_sample = conn.execute(
            """
            SELECT date, min_temperature, max_temperature, avg_temperature, total_precipitation, avg_wind_speed
            FROM daily_weather_summary
            ORDER BY date DESC
            LIMIT 7;
            """
        ).fetchall()

        md_summary = [
            f"### Tổng hợp phân tích ngày {partition_date} & gần đây",
            "| Ngày | Min (°C) | Max (°C) | Trung bình (°C) | Lượng mưa (mm) | Gió (km/h) |",
            "|:---:|:---:|:---:|:---:|:---:|:---:|",
        ]
        for r in summary_sample:
            md_summary.append(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} |")
        forecast_preview_md = "\n".join(md_summary)

    context.log.info(f"Daily weather mart updated for {partition_date}. Total summary days: {total_days}")
    return MaterializeResult(
        metadata={
            "dagster/table_name": MetadataValue.text("daily_weather_summary"),
            "dagster/column_schema": MetadataValue.table_schema(DAILY_SUMMARY_SCHEMA),
            "database_file": MetadataValue.path(DB_FILE_PATH),
            "partition_date": MetadataValue.text(partition_date),
            "total_days_summarized": MetadataValue.int(total_days),
            "dagster_run_id": MetadataValue.text(context.run.run_id or "manual"),
            "forecast_preview": MetadataValue.md(forecast_preview_md),
        }
    )
