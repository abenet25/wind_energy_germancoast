"""
fetch_smard_wind_offshore.py
=============================
Downloads daily offshore wind power generation data (Wind Offshore, realisiert)
from the public SMARD API (Bundesnetzagentur) for the period 2021-2025.

API base: https://www.smard.de/app/chart_data/
Documentation: https://smard.api.bund.dev/

Usage:
    python fetch_smard_wind_offshore.py

Output:
    data/raw/smard_wind_offshore_2021_2025.csv
"""

import requests
import pandas as pd
from datetime import datetime, timezone
import os
import time

# ── API parameters ─────────────────────────────────────────────────────────────
FILTER      = 1225        # Wind Offshore (realisiert)
REGION      = "DE"        # Germany (entire country)
RESOLUTION  = "day"       # Daily resolution

BASE_URL = "https://www.smard.de/app/chart_data"

# ── Time period of interest ────────────────────────────────────────────────────
START_DATE = datetime(2020, 12, 25, tzinfo=timezone.utc)
END_DATE   = datetime(2025, 12, 31, tzinfo=timezone.utc)

START_MS = int(START_DATE.timestamp() * 1000)
END_MS   = int(END_DATE.timestamp() * 1000)

# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR  = "data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "smard_wind_offshore_2021_2025.csv")


def get_available_timestamps():
    """Returns the list of available timestamps for the given filter and resolution."""
    url = f"{BASE_URL}/{FILTER}/{REGION}/index_{RESOLUTION}.json"
    print(f"Fetching available timestamps...\n  URL: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    timestamps = response.json().get("timestamps", [])
    print(f"  Found {len(timestamps)} available data blocks.")
    return timestamps


def get_timeseries_block(timestamp_ms):
    """Downloads a time series block starting from the given timestamp (in ms)."""
    url = (
        f"{BASE_URL}/{FILTER}/{REGION}/"
        f"{FILTER}_{REGION}_{RESOLUTION}_{timestamp_ms}.json"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json().get("series", [])


def main():
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1: fetch all available timestamps
    all_timestamps = get_available_timestamps()

    # Step 2: filter only those within our target period
    relevant_timestamps = [
        ts for ts in all_timestamps
        if START_MS <= ts <= END_MS
    ]
    print(f"\nBlocks within period 2021-2025: {len(relevant_timestamps)}")

    # Step 3: download each block and accumulate records
    all_records = []

    for i, ts_ms in enumerate(relevant_timestamps):
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        print(f"  [{i+1}/{len(relevant_timestamps)}] Downloading block: {dt.strftime('%Y-%m-%d')}")

        try:
            series = get_timeseries_block(ts_ms)
            for point in series:
                if len(point) == 2 and point[1] is not None:
                    point_dt = datetime.fromtimestamp(point[0] / 1000, tz=timezone.utc)
                    if START_MS <= point[0] <= END_MS:
                        all_records.append({
                            "timestamp_ms":      point[0],
                            "date":              point_dt.strftime("%Y-%m-%d"),
                            "wind_offshore_mwh": point[1]
                        })
        except requests.exceptions.RequestException as e:
            print(f"    WARNING: error fetching block {ts_ms}: {e}")

        # Small pause to avoid overloading the server
        time.sleep(0.3)

    # Step 4: convert to DataFrame and export
    if not all_records:
        print("\nWARNING: no data found. Check the time period or API parameters.")
        return

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    df = df.drop(columns=["timestamp_ms"])

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"\n✓ Data saved to: {OUTPUT_FILE}")
    print(f"  Total records: {len(df)}")
    print(f"  Period covered: {df['date'].min()} → {df['date'].max()}")
    print(f"\nFirst rows:")
    print(df.head())


if __name__ == "__main__":
    main()
