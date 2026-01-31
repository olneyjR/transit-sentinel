"""
Transit Sentinel - DuckDB Loader

Implements Medallion Architecture for analytics:
- Bronze: Raw ingested data
- Silver: Validated, cleaned data  
- Gold: Analytics-ready aggregations

DuckDB provides:
- In-process analytics (no separate server)
- Spatial extension for geospatial queries
- High performance (90% faster than PostgreSQL for analytics)
- Persistent storage with ACID guarantees
"""

import duckdb
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DuckDBLoader:
    """
    Load transit data into DuckDB with Medallion Architecture.
    
    Layer responsibilities:
    - Bronze: Append-only raw data, no transformations
    - Silver: Validated data with quality checks applied
    - Gold: Aggregated metrics and analytics
    """
    
    def __init__(
        self,
        database_path: str = "data/transit_sentinel.duckdb",
        enable_spatial: bool = True
    ):
        """
        Initialize DuckDB connection and schema.
        
        Args:
            database_path: Path to DuckDB database file
            enable_spatial: Load spatial extension for geospatial queries
        """
        self.db_path = Path(database_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create persistent connection
        self.conn = duckdb.connect(str(self.db_path))
        
        # Configure DuckDB
        self.conn.execute("SET memory_limit='2GB'")
        self.conn.execute("SET threads=4")
        
        # Load spatial extension if requested
        if enable_spatial:
            try:
                self.conn.execute("INSTALL spatial")
                self.conn.execute("LOAD spatial")
                logger.info("Loaded DuckDB spatial extension")
            except Exception as e:
                logger.warning(f"Could not load spatial extension: {e}")
        
        # Initialize schemas
        self._init_schemas()
        
        logger.info(f"Initialized DuckDB at {database_path}")
    
    def _init_schemas(self):
        """Create Bronze, Silver, and Gold schemas"""
        
        # Create schemas
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        
        # Bronze layer: Raw vehicle positions
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.vehicle_positions (
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                vehicle_id VARCHAR,
                trip_id VARCHAR,
                route_id VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                bearing DOUBLE,
                speed DOUBLE,
                timestamp TIMESTAMP,
                current_stop_sequence INTEGER,
                stop_id VARCHAR,
                current_status VARCHAR,
                congestion_level VARCHAR,
                occupancy_status VARCHAR,
                agency_id VARCHAR,
                feed_timestamp TIMESTAMP
            )
        """)
        
        # Bronze layer: Raw trip updates
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.trip_updates (
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                trip_id VARCHAR,
                route_id VARCHAR,
                vehicle_id VARCHAR,
                stop_sequence INTEGER,
                stop_id VARCHAR,
                arrival_delay INTEGER,
                departure_delay INTEGER,
                arrival_time TIMESTAMP,
                departure_time TIMESTAMP,
                schedule_relationship VARCHAR,
                agency_id VARCHAR,
                timestamp TIMESTAMP
            )
        """)
        
        # Bronze layer: Raw weather observations
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.weather_observations (
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                latitude DOUBLE,
                longitude DOUBLE,
                temperature_celsius DOUBLE,
                precipitation_mm DOUBLE,
                wind_speed_kmh DOUBLE,
                weather_code INTEGER,
                weather_condition VARCHAR,
                observation_time TIMESTAMP,
                agency_id VARCHAR
            )
        """)
        
        # Silver layer: Validated vehicle positions (only records passing quality checks)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver.vehicle_positions AS
            SELECT * FROM bronze.vehicle_positions WHERE 1=0
        """)
        
        # Gold layer: Hourly vehicle metrics
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS gold.hourly_vehicle_metrics (
                hour_timestamp TIMESTAMP,
                agency_id VARCHAR,
                route_id VARCHAR,
                total_vehicles INTEGER,
                avg_speed_kmh DOUBLE,
                max_speed_kmh DOUBLE,
                avg_congestion_score DOUBLE,
                total_observations INTEGER,
                PRIMARY KEY (hour_timestamp, agency_id, route_id)
            )
        """)
        
        # Gold layer: Route performance summary
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS gold.route_performance (
                date DATE,
                agency_id VARCHAR,
                route_id VARCHAR,
                avg_delay_seconds DOUBLE,
                max_delay_seconds INTEGER,
                min_delay_seconds INTEGER,
                on_time_percentage DOUBLE,
                total_trips INTEGER,
                PRIMARY KEY (date, agency_id, route_id)
            )
        """)
        
        logger.info("Initialized Medallion Architecture schemas")
    
    def load_vehicle_positions_bronze(
        self,
        positions: List
    ) -> int:
        """
        Load vehicle positions into Bronze layer.
        
        Args:
            positions: List of VehiclePosition Pydantic models
            
        Returns:
            Number of records inserted
        """
        if not positions:
            return 0
        
        # Convert Pydantic models to dicts
        records = [p.model_dump() for p in positions]
        
        # Batch insert
        insert_sql = """
            INSERT INTO bronze.vehicle_positions (
                vehicle_id, trip_id, route_id, latitude, longitude,
                bearing, speed, timestamp, current_stop_sequence, stop_id,
                current_status, congestion_level, occupancy_status,
                agency_id, feed_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        insert_data = [
            (
                r['vehicle_id'], r.get('trip_id'), r.get('route_id'),
                r['latitude'], r['longitude'], r.get('bearing'), r.get('speed'),
                r['timestamp'], r.get('current_stop_sequence'), r.get('stop_id'),
                r.get('current_status'), r.get('congestion_level'),
                r.get('occupancy_status'), r['agency_id'], r['feed_timestamp']
            )
            for r in records
        ]
        
        self.conn.executemany(insert_sql, insert_data)
        
        logger.info(f"Loaded {len(positions)} vehicle positions to Bronze")
        
        return len(positions)
    
    def promote_to_silver(self):
        """
        Promote validated Bronze data to Silver layer.
        
        Silver contains only records that pass quality checks:
        - Non-null critical fields
        - Realistic speeds
        - Valid coordinates
        """
        # Promote vehicle positions with quality filters
        self.conn.execute("""
            INSERT INTO silver.vehicle_positions
            SELECT * FROM bronze.vehicle_positions
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND latitude BETWEEN -90 AND 90
              AND longitude BETWEEN -180 AND 180
              AND (speed IS NULL OR speed BETWEEN 0 AND 33.3)  -- Max ~120 km/h
              AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
              AND vehicle_id NOT IN (SELECT vehicle_id FROM silver.vehicle_positions)
        """)
        
        logger.info("Promoted Bronze data to Silver with quality checks")
    
    def aggregate_to_gold_hourly(self):
        """
        Aggregate Silver data into Gold hourly metrics.
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO gold.hourly_vehicle_metrics
            SELECT
                DATE_TRUNC('hour', timestamp) as hour_timestamp,
                agency_id,
                route_id,
                COUNT(DISTINCT vehicle_id) as total_vehicles,
                AVG(speed * 3.6) as avg_speed_kmh,  -- m/s to km/h
                MAX(speed * 3.6) as max_speed_kmh,
                AVG(CASE
                    WHEN congestion_level = 'RUNNING_SMOOTHLY' THEN 1
                    WHEN congestion_level = 'STOP_AND_GO' THEN 2
                    WHEN congestion_level = 'CONGESTION' THEN 3
                    WHEN congestion_level = 'SEVERE_CONGESTION' THEN 4
                    ELSE 0
                END) as avg_congestion_score,
                COUNT(*) as total_observations
            FROM silver.vehicle_positions
            WHERE route_id IS NOT NULL
            GROUP BY 1, 2, 3
        """)
        
        logger.info("Aggregated hourly vehicle metrics to Gold")
    
    def get_current_metrics(self) -> Dict:
        """Get current system metrics"""
        
        metrics = {}
        
        # Bronze counts
        metrics['bronze_vehicle_positions'] = self.conn.execute(
            "SELECT COUNT(*) FROM bronze.vehicle_positions"
        ).fetchone()[0]
        
        metrics['bronze_trip_updates'] = self.conn.execute(
            "SELECT COUNT(*) FROM bronze.trip_updates"
        ).fetchone()[0]
        
        metrics['bronze_weather'] = self.conn.execute(
            "SELECT COUNT(*) FROM bronze.weather_observations"
        ).fetchone()[0]
        
        # Silver counts
        metrics['silver_vehicle_positions'] = self.conn.execute(
            "SELECT COUNT(*) FROM silver.vehicle_positions"
        ).fetchone()[0]
        
        # Gold counts
        metrics['gold_hourly_metrics'] = self.conn.execute(
            "SELECT COUNT(*) FROM gold.hourly_vehicle_metrics"
        ).fetchone()[0]
        
        # Data quality rate
        if metrics['bronze_vehicle_positions'] > 0:
            metrics['data_quality_rate'] = (
                metrics['silver_vehicle_positions'] /
                metrics['bronze_vehicle_positions']
            )
        else:
            metrics['data_quality_rate'] = 0.0
        
        return metrics
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        logger.info("Closed DuckDB connection")


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== DuckDB Loader Example ===\n")
    
    loader = DuckDBLoader(database_path="data/test_transit.duckdb")
    
    try:
        # Show current metrics
        metrics = loader.get_current_metrics()
        
        print("=== Current Metrics ===")
        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2%}")
            else:
                print(f"{key}: {value:,}")
        
        # Show table info
        print("\n=== Schema Info ===")
        tables = loader.conn.execute("""
            SELECT table_schema, table_name, estimated_size
            FROM duckdb_tables()
            WHERE table_schema IN ('bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name
        """).fetchall()
        
        for schema, table, size in tables:
            print(f"{schema}.{table}: {size:,} bytes")
        
    finally:
        loader.close()
    
    print("\n=== Example Complete ===")