"""
Transit Sentinel - Static GTFS Fetcher

Downloads and processes static GTFS data from Transitland API.
Static GTFS provides reference data: routes, stops, schedules, shapes.

This data is used to:
1. Validate real-time data (ensure vehicle is on a valid route)
2. Enrich analytics (route names, stop names, geographic shapes)
3. Calculate route performance metrics

Transitland V2 API: https://transit.land/api/v2
Free tier: 60 requests/minute, no API key required
"""

import requests
import zipfile
import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import time

logger = logging.getLogger(__name__)


class StaticGTFSFetcher:
    """
    Fetch and process static GTFS data from Transitland API.
    
    Transitland aggregates GTFS feeds from agencies worldwide and provides
    a clean API to access them without dealing with individual agency URLs.
    """
    
    def __init__(
        self,
        onestop_id: str,
        output_dir: str = "data/static_gtfs",
        transitland_api_base: str = "https://transit.land/api/v2"
    ):
        """
        Initialize static GTFS fetcher.
        
        Args:
            onestop_id: Transitland OnestopID for the operator (e.g., "o-c20n-trimet")
            output_dir: Directory to save downloaded GTFS files
            transitland_api_base: Transitland API base URL
        """
        self.onestop_id = onestop_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api_base = transitland_api_base
        
        # Rate limiting: 60 requests/minute = 1 per second
        self.last_request_time = 0
        self.min_request_interval = 1.0
        
        logger.info(f"Initialized StaticGTFSFetcher for {onestop_id}")
    
    def _rate_limit(self):
        """Enforce rate limiting for Transitland API"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> requests.Response:
        """Make rate-limited request to Transitland API"""
        self._rate_limit()
        
        url = f"{self.api_base}/{endpoint}"
        logger.debug(f"GET {url} with params {params}")
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        return response
    
    def get_feed_info(self) -> Dict:
        """
        Get information about the GTFS feed for this operator.
        
        Returns:
            Dict with feed metadata including download URL
        """
        logger.info(f"Fetching feed info for {self.onestop_id}")
        
        # Query feeds endpoint
        response = self._make_request(
            "rest/feeds",
            params={"onestop_id": self.onestop_id}
        )
        
        data = response.json()
        feeds = data.get("feeds", [])
        
        if not feeds:
            raise ValueError(f"No feeds found for operator {self.onestop_id}")
        
        # Take the first (usually only) feed
        feed = feeds[0]
        
        logger.info(
            f"Found feed: {feed.get('name', 'Unknown')} "
            f"(Spec: {feed.get('spec', 'Unknown')})"
        )
        
        return feed
    
    def download_gtfs(self, force: bool = False) -> Path:
        """
        Download static GTFS ZIP file.
        
        Args:
            force: If True, download even if file exists
            
        Returns:
            Path to downloaded ZIP file
        """
        feed_info = self.get_feed_info()
        
        # Transitland provides a static URL to the latest GTFS
        download_url = feed_info.get("urls", {}).get("static_current")
        
        if not download_url:
            raise ValueError(f"No static GTFS download URL found for {self.onestop_id}")
        
        # Save with timestamp
        timestamp = datetime.now().strftime("%Y%m%d")
        zip_filename = f"{self.onestop_id}_{timestamp}.zip"
        zip_path = self.output_dir / zip_filename
        
        if zip_path.exists() and not force:
            logger.info(f"GTFS file already exists: {zip_path}")
            return zip_path
        
        logger.info(f"Downloading GTFS from {download_url}")
        
        # Download with streaming to handle large files
        response = requests.get(download_url, stream=True, timeout=60)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(zip_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    progress = (downloaded / total_size) * 100
                    if downloaded % (1024 * 1024) == 0:  # Log every MB
                        logger.debug(f"Downloaded {downloaded:,} / {total_size:,} bytes ({progress:.1f}%)")
        
        logger.info(f"Downloaded {total_size:,} bytes to {zip_path}")
        
        return zip_path
    
    def extract_gtfs(self, zip_path: Path, extract_dir: Optional[Path] = None) -> Path:
        """
        Extract GTFS ZIP file to directory.
        
        Args:
            zip_path: Path to GTFS ZIP file
            extract_dir: Directory to extract to (default: same name as ZIP)
            
        Returns:
            Path to extraction directory
        """
        if extract_dir is None:
            extract_dir = self.output_dir / zip_path.stem
        
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Extracting {zip_path} to {extract_dir}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Log extracted files
        extracted_files = list(extract_dir.glob("*.txt"))
        logger.info(f"Extracted {len(extracted_files)} GTFS files: {[f.name for f in extracted_files]}")
        
        return extract_dir
    
    def load_stops(self, gtfs_dir: Path) -> List[Dict]:
        """
        Load stops.txt into memory.
        
        Args:
            gtfs_dir: Directory containing extracted GTFS files
            
        Returns:
            List of stop dictionaries
        """
        stops_file = gtfs_dir / "stops.txt"
        
        if not stops_file.exists():
            raise FileNotFoundError(f"stops.txt not found in {gtfs_dir}")
        
        stops = []
        
        with open(stops_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stops.append({
                    'stop_id': row['stop_id'],
                    'stop_name': row.get('stop_name', ''),
                    'stop_lat': float(row['stop_lat']),
                    'stop_lon': float(row['stop_lon']),
                    'location_type': row.get('location_type', '0'),
                    'parent_station': row.get('parent_station', '')
                })
        
        logger.info(f"Loaded {len(stops)} stops from {stops_file}")
        
        return stops
    
    def load_routes(self, gtfs_dir: Path) -> List[Dict]:
        """
        Load routes.txt into memory.
        
        Args:
            gtfs_dir: Directory containing extracted GTFS files
            
        Returns:
            List of route dictionaries
        """
        routes_file = gtfs_dir / "routes.txt"
        
        if not routes_file.exists():
            raise FileNotFoundError(f"routes.txt not found in {gtfs_dir}")
        
        routes = []
        
        with open(routes_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                routes.append({
                    'route_id': row['route_id'],
                    'route_short_name': row.get('route_short_name', ''),
                    'route_long_name': row.get('route_long_name', ''),
                    'route_type': int(row['route_type']),
                    'route_color': row.get('route_color', ''),
                    'route_text_color': row.get('route_text_color', '')
                })
        
        logger.info(f"Loaded {len(routes)} routes from {routes_file}")
        
        return routes
    
    def load_trips(self, gtfs_dir: Path) -> List[Dict]:
        """
        Load trips.txt into memory.
        
        Args:
            gtfs_dir: Directory containing extracted GTFS files
            
        Returns:
            List of trip dictionaries
        """
        trips_file = gtfs_dir / "trips.txt"
        
        if not trips_file.exists():
            raise FileNotFoundError(f"trips.txt not found in {gtfs_dir}")
        
        trips = []
        
        with open(trips_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                trips.append({
                    'trip_id': row['trip_id'],
                    'route_id': row['route_id'],
                    'service_id': row.get('service_id', ''),
                    'trip_headsign': row.get('trip_headsign', ''),
                    'direction_id': row.get('direction_id', ''),
                    'shape_id': row.get('shape_id', '')
                })
        
        logger.info(f"Loaded {len(trips)} trips from {trips_file}")
        
        return trips
    
    def fetch_and_process_all(self, force_download: bool = False) -> Dict[str, List[Dict]]:
        """
        Complete workflow: download, extract, and load all GTFS data.
        
        Args:
            force_download: Force re-download even if file exists
            
        Returns:
            Dict with 'stops', 'routes', 'trips' keys
        """
        logger.info(f"Starting complete GTFS fetch and process for {self.onestop_id}")
        
        # Download
        zip_path = self.download_gtfs(force=force_download)
        
        # Extract
        extract_dir = self.extract_gtfs(zip_path)
        
        # Load key files
        result = {
            'stops': self.load_stops(extract_dir),
            'routes': self.load_routes(extract_dir),
            'trips': self.load_trips(extract_dir)
        }
        
        logger.info(
            f"GTFS processing complete: "
            f"{len(result['stops'])} stops, "
            f"{len(result['routes'])} routes, "
            f"{len(result['trips'])} trips"
        )
        
        return result


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example: Fetch TriMet static GTFS
    print("=== Static GTFS Fetcher Example ===\n")
    
    fetcher = StaticGTFSFetcher(
        onestop_id="o-c20n-trimet",
        output_dir="data/static_gtfs"
    )
    
    try:
        # Fetch all data
        data = fetcher.fetch_and_process_all(force_download=False)
        
        # Display samples
        print(f"\n=== Sample Stops (first 3) ===")
        for stop in data['stops'][:3]:
            print(f"  {stop['stop_id']}: {stop['stop_name']} ({stop['stop_lat']}, {stop['stop_lon']})")
        
        print(f"\n=== Sample Routes (first 3) ===")
        for route in data['routes'][:3]:
            print(f"  {route['route_id']}: {route['route_short_name']} - {route['route_long_name']}")
        
        print(f"\n=== Sample Trips (first 3) ===")
        for trip in data['trips'][:3]:
            print(f"  {trip['trip_id']}: Route {trip['route_id']} to {trip['trip_headsign']}")
        
        print("\n=== Fetch Complete ===")
        
    except Exception as e:
        logger.error(f"Failed to fetch GTFS data: {e}", exc_info=True)