"""Dagster schedules for periodic execution and partitioned backfills."""

from dagster import (
    AssetSelection,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

# Job that materializes all weather assets
weather_pipeline_job = define_asset_job(
    name="weather_pipeline_job",
    selection=AssetSelection.all(),
    description="Job executing the full partitioned weather pipeline (raw -> cleaned -> marts).",
)

# Daily schedule built from the partitioned job (runs daily at 06:00 AM Asia/Ho_Chi_Minh)
weather_daily_schedule = build_schedule_from_partitioned_job(
    job=weather_pipeline_job,
    name="weather_daily_schedule",
    description="Daily schedule triggering the weather pipeline at 06:00 AM Asia/Ho_Chi_Minh.",
)
