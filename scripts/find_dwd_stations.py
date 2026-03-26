"""
find_dwd_stations.py
=====================
Reads the DWD station description file and filters stations
near the North Sea and Baltic Sea coast.

Usage:
    1. Download KL_Tageswerte_Beschreibung_Stationen.txt from:
       https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/
    2. Place it in the same folder as this script
    3. Run: python find_dwd_stations.py
"""

import pandas as pd

# ── Load station list ──────────────────────────────────────────────────────────
FILE = "KL_Tageswerte_Beschreibung_Stationen.txt"

df = pd.read_fwf(
    FILE,
    skiprows=2,          # skip header lines
    encoding="latin-1",
    header=None,
    names=["station_id", "date_from", "date_to", "altitude",
           "lat", "lon", "name", "state"]
)

# Drop rows with missing names
df = df.dropna(subset=["name"])

# ── Filter coastal / island stations relevant for North & Baltic Sea ───────────
keywords = [
    "Helgoland", "Sylt", "List", "Borkum", "Norderney", "Flensburg",
    "Schleswig", "Kiel", "Rostock", "Greifswald", "Stralsund", "Rügen",
    "Fehmarn", "Cuxhaven", "Emden", "Husum", "Büsum", "Westerland",
    "Warnemünde", "Arkona"
]

mask = df["name"].str.contains("|".join(keywords), case=False, na=False)
coastal = df[mask][["station_id", "date_from", "date_to", "lat", "lon", "name", "state"]]

print(f"Found {len(coastal)} coastal stations:\n")
print(coastal.to_string(index=False))
