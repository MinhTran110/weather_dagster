"""Dagster Definitions entrypoint assembling assets, checks, resources, and schedules."""

from dagster import Definitions

from weather_dagster.defs.assets import (
    cleaned_weather,
    daily_weather_summary,
    raw_weather,
)
from weather_dagster.defs.checks import (
    check_cleaned_weather_ranges,
    check_daily_summary_validity,
)
from weather_dagster.defs.resources import duckdb_resource
from weather_dagster.defs.schedules import (
    weather_daily_schedule,
    weather_pipeline_job,
)

defs = Definitions(
    assets=[
        raw_weather,
        cleaned_weather,
        daily_weather_summary,
    ],
    asset_checks=[
        check_cleaned_weather_ranges,
        check_daily_summary_validity,
    ],
    resources={
        "duckdb": duckdb_resource,
    },
    jobs=[
        weather_pipeline_job,
    ],
    schedules=[
        weather_daily_schedule,
    ],
)
