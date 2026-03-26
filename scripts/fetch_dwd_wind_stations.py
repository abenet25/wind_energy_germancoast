"""
fetch_dwd_wind_stations.py
===========================
Downloads daily climate data (KL) from the DWD Open Data server
for three island/exposed stations chosen for their open-sea wind exposure:

    - Helgoland       (island in the North Sea, ~70km offshore)
    - List auf Sylt   (northernmost tip of Sylt island, fully exposed)
    - Fehmarn         (Baltic Sea island, open exposure)

The script fetches from both endpoints and merges them:
    - historical/  → consolidated data up to end of 2024
    - recent/      → last ~500 days up to today

Data source:
    https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/

The KL files contain multiple climate variables. This script extracts:
    - date
    - station_id
    - station_name
    - sea
    - wind_speed_ms  (daily mean wind speed in m/s, column "FM")

Usage:
    python fetch_dwd_wind_stations.py

Output:
    data/raw/dwd_wind_stations_2021_2025.csv
"""

import requests
import zipfile
import io
import os
import pandas as pd

# ── Target stations ────────────────────────────────────────────────────────────
STATIONS = [
    ("02564", "Helgoland",     "North Sea"),
    ("03032", "List auf Sylt", "North Sea"),
    ("01503", "Fehmarn",       "Baltic Sea"),
]

# ── DWD base URLs ──────────────────────────────────────────────────────────────
BASE_URL_HISTORICAL = (
    "https://opendata.dwd.de/climate_environment/CDC/"
    "observations_germany/climate/daily/kl/historical/"
)
BASE_URL_RECENT = (
    "https://opendata.dwd.de/climate_environment/CDC/"
    "observations_germany/climate/daily/kl/recent/"
)

# ── Time period of interest ────────────────────────────────────────────────────
START_DATE = "2021-01-01"
END_DATE   = "2025-12-31"

# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR  = "data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dwd_wind_stations_2021_2025.csv")


def get_zip_filename(base_url, station_id):
    """
    Finds the correct zip filename for a station by listing the DWD directory
    and matching the station ID.
    """
    response = requests.get(base_url, timeout=30)
    response.raise_for_status()
    for line in response.text.splitlines():
        if f"_{station_id}_" in line and ".zip" in line:
            start = line.find('href="') + 6
            end   = line.find('"', start)
            return line[start:end]
    return None


def parse_zip(zip_content, station_id, station_name, sea):
    """
    Extracts the KL data file from a zip archive and returns a clean DataFrame
    with date and wind speed for the target period.
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        data_files = [f for f in z.namelist() if f.startswith("produkt_klima_tag_")]
        if not data_files:
            return None
        with z.open(data_files[0]) as f:
            df_raw = pd.read_csv(f, sep=";", encoding="latin-1")

    # Clean column names (DWD adds trailing spaces)
    df_raw.columns = df_raw.columns.str.strip()

    # Select and rename relevant columns
    df = df_raw[["MESS_DATUM", "FM"]].copy()
    df.columns = ["date", "wind_speed_ms"]

    # Parse date and filter to target period
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)].copy()

    # Replace DWD missing value flag with NaN
    df["wind_speed_ms"] = df["wind_speed_ms"].replace(-999, float("nan"))

    # Add station metadata
    df["station_id"]   = station_id
    df["station_name"] = station_name
    df["sea"]          = sea

    return df.reset_index(drop=True)


def download_station(station_id, station_name, sea):
    """
    Downloads and merges historical + recent data for a single station.
    Returns a combined DataFrame covering 2021-2025.
    """
    print(f"\nProcessing: {station_name} ({sea})")
    frames = []

    for label, base_url in [("historical", BASE_URL_HISTORICAL),
                             ("recent",     BASE_URL_RECENT)]:
        filename = get_zip_filename(base_url, station_id)
        if not filename:
            print(f"  WARNING: no {label} file found for station {station_id}")
            continue

        print(f"  [{label}] Downloading: {filename}")
        response = requests.get(base_url + filename, timeout=60)
        response.raise_for_status()
        print(f"  [{label}] Size: {len(response.content) / 1024:.1f} KB")

        df = parse_zip(response.content, station_id, station_name, sea)
        if df is not None and len(df) > 0:
            frames.append(df)
            print(f"  [{label}] Records in period: {len(df)}")
        else:
            print(f"  [{label}] No records in target period.")

    if not frames:
        return None

    # Merge historical + recent, drop duplicates (overlapping dates)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset="date").sort_values("date")
    return combined.reset_index(drop=True)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_frames = []

    for station_id, station_name, sea in STATIONS:
        df = download_station(station_id, station_name, sea)
        if df is not None:
            all_frames.append(df)

    if not all_frames:
        print("\nWARNING: no data retrieved. Check station IDs or network connection.")
        return

    # Combine all stations into one DataFrame
    result = pd.concat(all_frames, ignore_index=True)
    result = result[["date", "station_id", "station_name", "sea", "wind_speed_ms"]]
    result = result.sort_values(["station_name", "date"]).reset_index(drop=True)

    result.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"\n✓ Data saved to: {OUTPUT_FILE}")
    print(f"  Total records: {len(result)}")
    print(f"\nRecords per station:")
    print(result.groupby(["station_name", "sea"])["wind_speed_ms"].count().to_string())
    print(f"\nDate range per station:")
    for name, group in result.groupby("station_name"):
        print(f"  {name}: {group['date'].min()} → {group['date'].max()}")
    print(f"\nFirst rows:")
    print(result.head(10))


if __name__ == "__main__":
    main()
