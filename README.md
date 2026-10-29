# Weather Dagster Pipeline

Dự án data pipeline xây dựng bằng **Dagster** và **DuckDB**, thu thập dữ liệu thời tiết từ **Open-Meteo REST API**.

## Cấu trúc thư mục

```
weather_dagster/
├── pyproject.toml                     # Cấu hình uv, dependencies (dagster, duckdb, requests, pytest)
├── data/
│   └── weather.duckdb                 # File cơ sở dữ liệu DuckDB duy nhất
├── src/
│   └── weather_dagster/
│       ├── definitions.py             # Nạp Definitions (assets, checks, resources, schedules)
│       └── defs/
│           ├── assets.py              # Định nghĩa 3 tầng: raw -> cleaned -> marts
│           ├── checks.py              # Các Data Quality Check với @asset_check
│           ├── client.py              # Logic gọi Open-Meteo REST API (tách biệt khỏi asset)
│           ├── resources.py           # Khai báo DuckDBResource kết nối tới weather.duckdb
│           └── schedules.py           # Khai báo lịch chạy định kỳ (cron schedule)
└── tests/
    └── test_pipeline.py               # Chạy kiểm thử pytest độc lập không cần bật webserver
```

## Cài đặt & Chạy

### 1. Cài đặt dependencies với `uv` hoặc `pip`
```bash
# Với uv:
uv sync

# Hoặc với pip:
pip install -e ".[dev]"
```

### 2. Chạy Dagster Webserver (UI)
```bash
dagster dev
```
Truy cập giao diện tại: `http://localhost:3000`

### 3. Chạy kiểm thử pytest độc lập (không cần webserver)
```bash
pytest
```
