"""Dagster schedules for periodic execution of the weather pipeline."""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)

# Job that materializes all weather assets
weather_pipeline_job = define_asset_job(
    name="weather_pipeline_job",
    selection=AssetSelection.all(),
    description="Job executing the full weather data pipeline (raw -> cleaned -> marts).",
)

# Daily schedule running at 06:00 AM (Asia/Ho_Chi_Minh) every day
weather_daily_schedule = ScheduleDefinition(
    name="weather_daily_schedule",
    job=weather_pipeline_job,
    cron_schedule="0 * * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    description="Daily schedule triggering the full weather data pipeline at 06:00 AM Vietnam time.",
)
