"""Data Quality Checks using Dagster @asset_check with audit validation."""

from dagster import AssetCheckResult, MetadataValue, asset_check
from dagster_duckdb import DuckDBResource

from weather_dagster.defs.assets import cleaned_weather, daily_weather_summary


@asset_check(
    asset=cleaned_weather,
    description="Validate that cleaned weather metrics and audit fields are complete and within reasonable ranges.",
)
def check_cleaned_weather_ranges(duckdb: DuckDBResource) -> AssetCheckResult:
    """Verify temperature, humidity, timestamps, and audit metadata in cleaned_weather."""
    with duckdb.get_connection() as conn:
        # Check null primary key / audit fields
        missing_identity_rows = conn.execute(
            """
            SELECT COUNT(*) FROM cleaned_weather
            WHERE timestamp IS NULL
               OR location_name IS NULL
               OR source IS NULL
               OR created_at IS NULL
               OR updated_at IS NULL;
            """
        ).fetchone()[0]

        # Check out-of-range temperatures (-50°C to 60°C)
        invalid_temps = conn.execute(
            """
            SELECT COUNT(*) FROM cleaned_weather
            WHERE temperature_2m IS NOT NULL AND (temperature_2m < -50.0 OR temperature_2m > 60.0);
            """
        ).fetchone()[0]

        # Check invalid humidity (< 0% or > 100%)
        invalid_humidity = conn.execute(
            """
            SELECT COUNT(*) FROM cleaned_weather
            WHERE relative_humidity_2m IS NOT NULL AND (relative_humidity_2m < 0.0 OR relative_humidity_2m > 100.0);
            """
        ).fetchone()[0]

        # Check negative precipitation
        negative_precip = conn.execute(
            "SELECT COUNT(*) FROM cleaned_weather WHERE precipitation < 0.0;"
        ).fetchone()[0]

        total_rows = conn.execute("SELECT COUNT(*) FROM cleaned_weather;").fetchone()[0]

    passed = (
        total_rows > 0
        and missing_identity_rows == 0
        and invalid_temps == 0
        and invalid_humidity == 0
        and negative_precip == 0
    )

    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_rows": MetadataValue.int(total_rows),
            "missing_identity_rows": MetadataValue.int(missing_identity_rows),
            "invalid_temperatures": MetadataValue.int(invalid_temps),
            "invalid_humidity": MetadataValue.int(invalid_humidity),
            "negative_precipitation": MetadataValue.int(negative_precip),
        },
    )


@asset_check(
    asset=daily_weather_summary,
    description="Validate that daily aggregated weather summary is non-empty, has audit fields, and consistent values.",
)
def check_daily_summary_validity(duckdb: DuckDBResource) -> AssetCheckResult:
    """Verify daily min temperature <= max temperature, audit fields are populated, and table is non-empty."""
    with duckdb.get_connection() as conn:
        total_days = conn.execute("SELECT COUNT(*) FROM daily_weather_summary;").fetchone()[0]

        # Missing identity / audit fields
        missing_identity = conn.execute(
            """
            SELECT COUNT(*) FROM daily_weather_summary
            WHERE date IS NULL OR location_name IS NULL OR created_at IS NULL OR updated_at IS NULL;
            """
        ).fetchone()[0]

        # Inconsistent temp records where min > max
        inconsistent_temps = conn.execute(
            """
            SELECT COUNT(*) FROM daily_weather_summary
            WHERE min_temperature > max_temperature;
            """
        ).fetchone()[0]

    passed = total_days > 0 and missing_identity == 0 and inconsistent_temps == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_days_summarized": MetadataValue.int(total_days),
            "missing_identity_fields": MetadataValue.int(missing_identity),
            "inconsistent_temperatures": MetadataValue.int(inconsistent_temps),
        },
    )
