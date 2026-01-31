#!/usr/bin/env python3
"""
Download real TriMet GTFS data and load it
"""

import requests
import zipfile
import csv
from pathlib import Path

print("=" * 60)
print("  DOWNLOADING REAL TRIMET GTFS DATA")
print("=" * 60)

# Create data directory
data_dir = Path("data/trimet_gtfs")
data_dir.mkdir(parents=True, exist_ok=True)

# Download GTFS zip
print("\n[STEP 1] Downloading GTFS archive from TriMet...")
url = "https://developer.trimet.org/schedule/gtfs.zip"
zip_path = Path("data/trimet.zip")

response = requests.get(url, timeout=60)
response.raise_for_status()

with open(zip_path, 'wb') as f:
    f.write(response.content)

print(f"  Downloaded {len(response.content):,} bytes")

# Extract
print("\n[STEP 2] Extracting GTFS files...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(data_dir)

files = list(data_dir.glob("*.txt"))
print(f"  Extracted {len(files)} files:")
for f in sorted(files):
    print(f"    - {f.name}")

# Load and show routes
print("\n[STEP 3] Loading routes...")
routes_file = data_dir / "routes.txt"

routes = []
with open(routes_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        routes.append(row)

print(f"  Found {len(routes)} routes")
print("\n  Sample routes:")
for route in routes[:10]:
    print(f"    Route {route['route_short_name']}: {route['route_long_name']}")

# Load and show stops
print("\n[STEP 4] Loading stops...")
stops_file = data_dir / "stops.txt"

stops = []
with open(stops_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        stops.append(row)

print(f"  Found {len(stops)} stops")
print("\n  Sample stops:")
for stop in stops[:10]:
    print(f"    {stop['stop_name']}: ({stop['stop_lat']}, {stop['stop_lon']})")

print("\n" + "=" * 60)
print("  ✓ REAL TRIMET DATA DOWNLOADED!")
print("=" * 60)
print(f"\nData saved to: {data_dir}")
print("\nYou now have REAL:")
print(f"  - {len(routes)} transit routes")
print(f"  - {len(stops)} bus/rail stops")
print("  - Schedules, shapes, and more")
print("\nThis is actual production data from Portland's transit system!")