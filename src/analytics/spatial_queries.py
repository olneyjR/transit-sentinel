"""
Transit Sentinel - Spatial Analytics

Geospatial queries using DuckDB spatial extension.
Demonstrates advanced analytical capabilities for portfolio.
"""

import duckdb
import logging
import math
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class SpatialAnalytics:
    """Geospatial analytics for transit data"""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        logger.info("Initialized Spatial Analytics")
    
    def haversine_distance(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float
    ) -> float:
        """
        Calculate Haversine distance between two points (in meters).
        """
        R = 6371000  # Earth radius in meters
        
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_phi/2) ** 2 +
             math.cos(phi1) * math.cos(phi2) *
             math.sin(delta_lambda/2) ** 2)
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c
    
    def vehicles_near_point(
        self,
        latitude: float,
        longitude: float,
        radius_meters: float = 1000
    ) -> List[Dict]:
        """Find vehicles within radius of a point"""
        
        query = """
            SELECT
                vehicle_id,
                route_id,
                latitude,
                longitude,
                speed,
                timestamp,
                6371000 * 2 * ASIN(SQRT(
                    POW(SIN((RADIANS(?) - RADIANS(latitude))/2), 2) +
                    COS(RADIANS(?)) * COS(RADIANS(latitude)) *
                    POW(SIN((RADIANS(?) - RADIANS(longitude))/2), 2)
                )) as distance_meters
            FROM silver.vehicle_positions
            WHERE 6371000 * 2 * ASIN(SQRT(
                    POW(SIN((RADIANS(?) - RADIANS(latitude))/2), 2) +
                    COS(RADIANS(?)) * COS(RADIANS(latitude)) *
                    POW(SIN((RADIANS(?) - RADIANS(longitude))/2), 2)
                )) <= ?
            ORDER BY distance_meters
        """
        
        results = self.conn.execute(
            query,
            [latitude, latitude, longitude, 
             latitude, latitude, longitude, radius_meters]
        ).fetchall()
        
        return [
            {
                'vehicle_id': r[0],
                'route_id': r[1],
                'latitude': r[2],
                'longitude': r[3],
                'speed': r[4],
                'timestamp': r[5],
                'distance_meters': r[6]
            }
            for r in results
        ]
    
    def generate_heat_map_grid(
        self,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        grid_size: int = 20
    ) -> List[Dict]:
        """
        Generate heat map of vehicle density.
        
        Returns grid cells with vehicle counts.
        """
        
        query = """
            WITH grid AS (
                SELECT
                    FLOOR((latitude - ?) / (? - ?) * ?) as lat_cell,
                    FLOOR((longitude - ?) / (? - ?) * ?) as lon_cell
                FROM silver.vehicle_positions
                WHERE latitude BETWEEN ? AND ?
                  AND longitude BETWEEN ? AND ?
            )
            SELECT
                lat_cell,
                lon_cell,
                COUNT(*) as vehicle_count,
                ? + (lat_cell / ?) * (? - ?) as cell_center_lat,
                ? + (lon_cell / ?) * (? - ?) as cell_center_lon
            FROM grid
            GROUP BY lat_cell, lon_cell
            ORDER BY vehicle_count DESC
        """
        
        results = self.conn.execute(
            query,
            [min_lat, max_lat, min_lat, grid_size,
             min_lon, max_lon, min_lon, grid_size,
             min_lat, max_lat, min_lon, max_lon,
             min_lat, grid_size, max_lat, min_lat,
             min_lon, grid_size, max_lon, min_lon]
        ).fetchall()
        
        return [
            {
                'lat_cell': r[0],
                'lon_cell': r[1],
                'vehicle_count': r[2],
                'center_lat': r[3],
                'center_lon': r[4]
            }
            for r in results
        ]
    
    def identify_slow_zones(
        self,
        speed_threshold_kmh: float = 10.0
    ) -> List[Dict]:
        """Identify geographic areas with slow vehicle speeds"""
        
        query = """
            SELECT
                ROUND(latitude, 2) as lat_zone,
                ROUND(longitude, 2) as lon_zone,
                AVG(speed * 3.6) as avg_speed_kmh,
                COUNT(*) as observation_count,
                STRING_AGG(DISTINCT route_id, ', ') as affected_routes
            FROM silver.vehicle_positions
            WHERE speed IS NOT NULL
            GROUP BY lat_zone, lon_zone
            HAVING avg_speed_kmh < ?
            ORDER BY avg_speed_kmh ASC
            LIMIT 50
        """
        
        results = self.conn.execute(query, [speed_threshold_kmh]).fetchall()
        
        return [
            {
                'lat_zone': r[0],
                'lon_zone': r[1],
                'avg_speed_kmh': r[2],
                'observation_count': r[3],
                'affected_routes': r[4]
            }
            for r in results
        ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Spatial Analytics module ready")
    print("Example usage:")
    print("""
    import duckdb
    from spatial_queries import SpatialAnalytics
    
    conn = duckdb.connect('data/transit_sentinel.duckdb')
    analytics = SpatialAnalytics(conn)
    
    # Find vehicles near downtown Portland
    vehicles = analytics.vehicles_near_point(45.5152, -122.6784, 1000)
    
    # Identify slow zones
    slow_zones = analytics.identify_slow_zones(speed_threshold_kmh=10.0)
    """)