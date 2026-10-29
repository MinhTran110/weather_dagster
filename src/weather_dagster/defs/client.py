"""Open-Meteo REST API client logic separated from Dagster assets."""

from typing import Any, Dict, List, Optional
import requests


class OpenMeteoClient:
    """Client for fetching weather data from Open-Meteo REST API."""

    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, timeout: int = 30, session: Optional[requests.Session] = None):
        self.timeout = timeout
        self.session = session or requests.Session()

    def fetch_weather(
        self,
        latitude: float = 21.0285,  # Hanoi latitude
        longitude: float = 105.8542,  # Hanoi longitude
        hourly: Optional[List[str]] = None,
        timezone: str = "auto",
        forecast_days: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch weather data from Open-Meteo API for forecast or historical partitions.

        Args:
            latitude: Latitude of the location.
            longitude: Longitude of the location.
            hourly: List of hourly weather variables to request.
            timezone: Timezone string, defaults to "auto".
            forecast_days: Number of forecast days to retrieve (1-16).
            start_date: Start date string in YYYY-MM-DD format (for partitions/backfills).
            end_date: End date string in YYYY-MM-DD format (for partitions/backfills).

        Returns:
            Dict containing API response payload with hourly weather metrics.

        Raises:
            requests.RequestException: If the HTTP request fails.
        """
        if hourly is None:
            hourly = [
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "wind_speed_10m",
            ]

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(hourly),
            "timezone": timezone,
        }

        # If backfill / specific partition dates are requested
        if start_date and end_date:
            params["start_date"] = start_date
            params["end_date"] = end_date
        else:
            params["forecast_days"] = forecast_days or 7

        response = self.session.get(self.FORECAST_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
