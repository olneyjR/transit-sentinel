#!/usr/bin/env python3
"""
Transit Sentinel - Simple End-to-End Test

This script runs a minimal end-to-end test to verify the system is working.

Steps:
1. Poll GTFS-RT feed for vehicle positions
2. Fetch current weather
3. Validate data with Pydantic schemas
4. Load into DuckDB
5. Run basic analytics query
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from ingestion.realtime_gtfs_poller import GTFSRealtimePoller
from ingestion.weather_enrichment import WeatherEnrichment
from analytics.duckdb_loader import DuckDBLoader

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Only show warnings/errors for clean output
    format='%(levelname)s: %(message)s'
)

def print_step(step_num: int, total: int, description: str):
    """Print step header"""
    print(f"\nStep {step_num}/{total}: {description}")
    print("-" * 50)

def print_success(message: str):
    """Print success message"""
    print(f"  ✓ {message}")

def print_error(message: str):
    """Print error message"""
    print(f"  ✗ {message}")

def main():
    print("=" * 60)
    print("  TRANSIT SENTINEL - SIMPLE END-TO-END TEST")
    print("=" * 60)
    
    try:
        # Step 1: Poll GTFS-RT feed
        print_step(1, 5, "Polling GTFS-RT feed...")
        
        poller = GTFSRealtimePoller(
            feed_url="https://developer.trimet.org/ws/gtfsrt/VehiclePositions",
            agency_id="trimet"
        )
        
        result = poller.poll_once()
        vehicle_positions = result['vehicle_positions']
        
        if not vehicle_positions:
            print_error("No vehicle positions received - feed may be down or outside service hours")
            print("  Tip: TriMet operates ~5 AM - 1 AM Pacific Time")
            return 1
        
        print_success(f"Retrieved {len(vehicle_positions)} vehicle positions")
        
        # Show sample
        if vehicle_positions:
            sample = vehicle_positions[0]
            print(f"  Sample: Vehicle {sample.vehicle_id} at ({sample.latitude:.4f}, {sample.longitude:.4f})")
        
        # Step 2: Fetch weather
        print_step(2, 5, "Fetching weather data...")
        
        weather_service = WeatherEnrichment()
        weather = weather_service.fetch_current_weather(
            latitude=45.5152,  # Portland center
            longitude=-122.6784,
            agency_id="trimet"
        )
        
        if weather:
            print_success(f"Weather: {weather.temperature_celsius}°C, {weather.weather_condition}")
        else:
            print_error("Could not fetch weather data")
        
        # Step 3: Validate data
        print_step(3, 5, "Validating data quality...")
        
        valid_count = len(vehicle_positions)
        total_count = poller.stats['total_entities']
        validation_rate = (valid_count / total_count * 100) if total_count > 0 else 0
        
        print_success(f"Valid records: {valid_count}/{total_count} ({validation_rate:.1f}%)")
        
        if validation_rate < 90:
            print_error(f"Warning: Validation rate below 90% - check data quality settings")
        
        # Step 4: Load to DuckDB
        print_step(4, 5, "Loading to DuckDB...")
        
        db_path = Path("data/test_transit.duckdb")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        loader = DuckDBLoader(database_path=str(db_path))
        
        # Load to Bronze
        loaded_count = loader.load_vehicle_positions_bronze(vehicle_positions)
        print_success(f"Loaded {loaded_count} records to Bronze layer")
        
        # Promote to Silver
        loader.promote_to_silver()
        print_success("Promoted validated records to Silver layer")
        
        # Step 5: Run analytics
        print_step(5, 5, "Running basic analytics...")
        
        # Get metrics
        metrics = loader.get_current_metrics()
        
        print_success(f"Bronze records: {metrics['bronze_vehicle_positions']:,}")
        print_success(f"Silver records: {metrics['silver_vehicle_positions']:,}")
        print_success(f"Data quality rate: {metrics['data_quality_rate']:.1%}")
        
        # Query some stats
        stats = loader.conn.execute("""
            SELECT
                COUNT(DISTINCT vehicle_id) as active_vehicles,
                COUNT(DISTINCT route_id) as routes_covered,
                AVG(speed * 3.6) as avg_speed_kmh,
                MAX(speed * 3.6) as max_speed_kmh
            FROM silver.vehicle_positions
            WHERE speed IS NOT NULL
        """).fetchone()
        
        if stats:
            print_success(f"Active vehicles: {stats[0]}")
            print_success(f"Routes covered: {stats[1]}")
            print_success(f"Average speed: {stats[2]:.1f} km/h")
            print_success(f"Max speed: {stats[3]:.1f} km/h")
        
        loader.close()
        
        # Success summary
        print("\n" + "=" * 60)
        print("  ✅ TEST COMPLETE - TRANSIT SENTINEL IS WORKING!")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Run 'python run_pipeline.py' for continuous operation")
        print("  2. View data in DuckDB: data/test_transit.duckdb")
        print("  3. Access Grafana dashboards: http://localhost:3000")
        print("  4. Read README.md for detailed documentation")
        print()
        
        return 0
        
    except Exception as e:
        print_error(f"Test failed: {str(e)}")
        logging.exception("Detailed error:")
        
        print("\n" + "=" * 60)
        print("  ❌ TEST FAILED - See error above")
        print("=" * 60)
        print("\nTroubleshooting:")
        print("  1. Check Docker is running: docker-compose ps")
        print("  2. Verify internet connection")
        print("  3. Check logs: tail -f logs/transit-sentinel.log")
        print("  4. See QUICKSTART.md for common issues")
        print()
        
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)