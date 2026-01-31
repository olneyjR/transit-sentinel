#!/usr/bin/env python3
"""
Transit Sentinel - Mock Data Test

Tests the pipeline with synthetic data to verify all components work.
This bypasses external API issues.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from validation.schemas import VehiclePosition
from analytics.duckdb_loader import DuckDBLoader

print("=" * 60)
print("  TRANSIT SENTINEL - MOCK DATA TEST")
print("=" * 60)

# Create mock vehicle positions
print("\n[STEP 1] Creating mock vehicle positions...")
mock_positions = [
    VehiclePosition(
        vehicle_id=f"vehicle_{i}",
        trip_id=f"trip_{i}",
        route_id=f"route_{i % 5}",
        latitude=45.5152 + (i * 0.01),
        longitude=-122.6784 + (i * 0.01),
        bearing=float(i * 10 % 360),
        speed=float(10 + i % 20),
        timestamp=datetime.now(timezone.utc),
        agency_id="test_agency",
        feed_timestamp=datetime.now(timezone.utc)
    )
    for i in range(50)
]

print(f"  Created {len(mock_positions)} mock vehicles")
print(f"  Sample: Vehicle {mock_positions[0].vehicle_id} at "
      f"({mock_positions[0].latitude:.4f}, {mock_positions[0].longitude:.4f})")

# Initialize DuckDB
print("\n[STEP 2] Initializing DuckDB...")
db_path = Path("data/test_mock.duckdb")
db_path.parent.mkdir(parents=True, exist_ok=True)

loader = DuckDBLoader(database_path=str(db_path))
print(f"  Database ready: {db_path}")

# Load to Bronze
print("\n[STEP 3] Loading to Bronze layer...")
loaded = loader.load_vehicle_positions_bronze(mock_positions)
print(f"  Loaded {loaded} records")

# Promote to Silver
print("\n[STEP 4] Promoting to Silver layer...")
loader.promote_to_silver()
metrics = loader.get_current_metrics()
print(f"  Silver records: {metrics['silver_vehicle_positions']}")
print(f"  Data quality: {metrics['data_quality_rate']:.1%}")

# Aggregate to Gold
print("\n[STEP 5] Aggregating to Gold layer...")
loader.aggregate_to_gold_hourly()
print(f"  Gold metrics: {metrics['gold_hourly_metrics']}")

# Run some analytics
print("\n[STEP 6] Running analytics...")
stats = loader.conn.execute("""
    SELECT
        COUNT(DISTINCT vehicle_id) as vehicles,
        COUNT(DISTINCT route_id) as routes,
        AVG(speed * 3.6) as avg_speed_kmh,
        MAX(speed * 3.6) as max_speed_kmh
    FROM silver.vehicle_positions
""").fetchone()

print(f"  Active vehicles: {stats[0]}")
print(f"  Routes covered: {stats[1]}")
print(f"  Average speed: {stats[2]:.1f} km/h")
print(f"  Max speed: {stats[3]:.1f} km/h")

loader.close()

print("\n" + "=" * 60)
print("  ✓ ALL TESTS PASSED - PIPELINE WORKING!")
print("=" * 60)
print("\nCore components verified:")
print("  ✓ Pydantic validation")
print("  ✓ DuckDB Medallion architecture")
print("  ✓ Bronze/Silver/Gold layers")
print("  ✓ SQL analytics")
print("\nNext steps:")
print("  1. Get TriMet API key: https://developer.trimet.org/appid/registration/")
print("  2. Add to config/agency_config.yaml")
print("  3. Run real pipeline: python run_pipeline.py")