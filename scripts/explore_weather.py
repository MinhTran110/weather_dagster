"""Utility script for querying and analyzing weather data directly from DuckDB."""

from pathlib import Path
import duckdb

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "weather.duckdb"


def analyze_weather():
    if not DB_PATH.exists():
        print(f"Error: Database file not found at {DB_PATH}")
        return

    conn = duckdb.connect(str(DB_PATH), read_only=True)

    print("=" * 85)
    print(" 🌤️  BÁO CÁO PHÂN TÍCH THỜI TIẾT TỔNG HỢP (TẦNG MARTS: daily_weather_summary)")
    print("=" * 85)

    query_marts = """
        SELECT 
            location_name AS 'Địa điểm',
            date AS 'Ngày',
            min_temperature AS 'Min (°C)',
            max_temperature AS 'Max (°C)',
            avg_temperature AS 'Trung bình (°C)',
            total_precipitation AS 'Tổng mưa (mm)',
            avg_wind_speed AS 'Gió TB (km/h)',
            record_count AS 'Số giờ đo'
        FROM daily_weather_summary
        ORDER BY date;
    """
    conn.execute(query_marts)
    rows = conn.fetchall()
    cols = [desc[0] for desc in conn.description]

    # Print table header
    header_fmt = "{:<10} {:<12} {:<10} {:<10} {:<16} {:<16} {:<14} {:<10}"
    print(header_fmt.format(*cols))
    print("-" * 100)
    for r in rows:
        print(header_fmt.format(str(r[0]), str(r[1]), f"{r[2]:.1f}", f"{r[3]:.1f}", f"{r[4]:.2f}", f"{r[5]:.1f}", f"{r[6]:.2f}", str(r[7])))

    print("\n" + "=" * 85)
    print(" 🔍 CÁC CHỈ SỐ ĐẶC BIỆT CẦN LƯU Ý TRONG 7 NGÀY TỚI")
    print("=" * 85)

    # Highest temperature day
    max_day = conn.execute("""
        SELECT date, max_temperature FROM daily_weather_summary ORDER BY max_temperature DESC LIMIT 1;
    """).fetchone()
    print(f"🔥 Ngày nóng nhất: {max_day[0]} với nhiệt độ cao nhất {max_day[1]}°C")

    # Lowest temperature day
    min_day = conn.execute("""
        SELECT date, min_temperature FROM daily_weather_summary ORDER BY min_temperature ASC LIMIT 1;
    """).fetchone()
    print(f"❄️ Ngày mát nhất: {min_day[0]} với nhiệt độ thấp nhất {min_day[1]}°C")

    # Heaviest rain day
    rain_day = conn.execute("""
        SELECT date, total_precipitation FROM daily_weather_summary ORDER BY total_precipitation DESC LIMIT 1;
    """).fetchone()
    print(f"🌧️ Ngày mưa nhiều nhất: {rain_day[0]} với tổng lượng mưa {rain_day[1]} mm")

    print("\n" + "=" * 85)
    print(" 🕒 XU HƯỚNG THEO GIỜ HÔM NAY (TẦNG CLEANED: cleaned_weather)")
    print("=" * 85)

    query_today = """
        SELECT 
            strftime(timestamp, '%H:%M') AS 'Giờ',
            temperature_2m AS 'Nhiệt độ (°C)',
            relative_humidity_2m AS 'Độ ẩm (%)',
            precipitation AS 'Lượng mưa (mm)',
            wind_speed_10m AS 'Gió (km/h)'
        FROM cleaned_weather
        WHERE CAST(timestamp AS DATE) = (SELECT MIN(CAST(timestamp AS DATE)) FROM cleaned_weather)
        ORDER BY timestamp
        LIMIT 8;
    """
    conn.execute(query_today)
    today_rows = conn.fetchall()
    today_cols = [desc[0] for desc in conn.description]
    fmt_today = "{:<8} {:<16} {:<14} {:<18} {:<12}"
    print(fmt_today.format(*today_cols))
    print("-" * 75)
    for r in today_rows:
        print(fmt_today.format(str(r[0]), f"{r[1]:.1f}", f"{r[2]:.1f}", f"{r[3]:.1f}", f"{r[4]:.1f}"))

    conn.close()


if __name__ == "__main__":
    analyze_weather()
