#!/usr/bin/env python3
"""
Load real TriMet GTFS data into DuckDB and run analytics
"""

import duckdb
from pathlib import Path

print("=" * 60)
print("  ANALYZING REAL TRIMET DATA")
print("=" * 60)

# Connect to DuckDB
db_path = Path("data/trimet_analysis.duckdb")
conn = duckdb.connect(str(db_path))

# Load spatial extension
try:
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    print("\n✓ DuckDB spatial extension loaded")
except:
    print("\n! Spatial extension already installed")

# Load routes
print("\n[STEP 1] Loading routes into DuckDB...")
conn.execute("""
    CREATE OR REPLACE TABLE routes AS
    SELECT * FROM read_csv_auto('data/trimet_gtfs/routes.txt')
""")

route_count = conn.execute("SELECT COUNT(*) FROM routes").fetchone()[0]
print(f"  Loaded {route_count} routes")

# Load stops
print("\n[STEP 2] Loading stops into DuckDB...")
conn.execute("""
    CREATE OR REPLACE TABLE stops AS
    SELECT * FROM read_csv_auto('data/trimet_gtfs/stops.txt')
""")

stop_count = conn.execute("SELECT COUNT(*) FROM stops").fetchone()[0]
print(f"  Loaded {stop_count} stops")

# Analytics
print("\n[ANALYSIS] Route Statistics:")
print("-" * 60)

# Routes by type
results = conn.execute("""
    SELECT 
        CASE route_type
            WHEN 0 THEN 'Tram/Light Rail'
            WHEN 1 THEN 'Subway/Metro'
            WHEN 3 THEN 'Bus'
            ELSE 'Other'
        END as type,
        COUNT(*) as count
    FROM routes
    GROUP BY route_type
    ORDER BY count DESC
""").fetchall()

for row in results:
    print(f"  {row[0]}: {row[1]} routes")

# Geographic analysis
print("\n[ANALYSIS] Geographic Coverage:")
print("-" * 60)

bounds = conn.execute("""
    SELECT 
        MIN(stop_lat) as min_lat,
        MAX(stop_lat) as max_lat,
        MIN(stop_lon) as min_lon,
        MAX(stop_lon) as max_lon
    FROM stops
""").fetchone()

print(f"  Latitude range: {bounds[0]:.4f} to {bounds[1]:.4f}")
print(f"  Longitude range: {bounds[2]:.4f} to {bounds[3]:.4f}")

# Downtown stops
print("\n[ANALYSIS] Stops in Downtown Portland:")
print("-" * 60)

downtown_stops = conn.execute("""
    SELECT stop_name, stop_lat, stop_lon
    FROM stops
    WHERE stop_lat BETWEEN 45.50 AND 45.53
      AND stop_lon BETWEEN -122.69 AND -122.66
    LIMIT 10
""").fetchall()

for stop in downtown_stops:
    print(f"  {stop[0]}: ({stop[1]:.4f}, {stop[2]:.4f})")

# Busiest areas (most stops)
print("\n[ANALYSIS] Areas with Most Stops (by 0.01° grid):")
print("-" * 60)

busy_areas = conn.execute("""
    SELECT 
        ROUND(stop_lat, 2) as lat_grid,
        ROUND(stop_lon, 2) as lon_grid,
        COUNT(*) as stop_count
    FROM stops
    GROUP BY lat_grid, lon_grid
    ORDER BY stop_count DESC
    LIMIT 10
""").fetchall()

for area in busy_areas:
    print(f"  Grid ({area[0]}, {area[1]}): {area[2]} stops")

# Route lengths (approximate)
print("\n[ANALYSIS] Longest Routes by Stop Count:")
print("-" * 60)

# Load stop_times and trips
conn.execute("""
    CREATE OR REPLACE TABLE stop_times AS
    SELECT * FROM read_csv_auto('data/trimet_gtfs/stop_times.txt')
""")

conn.execute("""
    CREATE OR REPLACE TABLE trips AS
    SELECT * FROM read_csv_auto('data/trimet_gtfs/trips.txt')
""")

route_stops = conn.execute("""
    SELECT 
        r.route_short_name,
        r.route_long_name,
        COUNT(DISTINCT st.stop_id) as unique_stops
    FROM routes r
    JOIN trips t ON r.route_id = t.route_id
    JOIN stop_times st ON t.trip_id = st.trip_id
    GROUP BY r.route_short_name, r.route_long_name
    ORDER BY unique_stops DESC
    LIMIT 10
""").fetchall()

for route in route_stops:
    print(f"  Route {route[0]} ({route[1]}): {route[2]} stops")

print("\n" + "=" * 60)
print("  ✓ REAL DATA ANALYSIS COMPLETE!")
print("=" * 60)
print(f"\nDatabase saved to: {db_path}")
print("\nYou can now query this data:")
print("  python")
print("  >>> import duckdb")
print("  >>> conn = duckdb.connect('data/trimet_analysis.duckdb')")
print("  >>> conn.execute('SELECT * FROM routes LIMIT 5').fetchall()")

conn.close()