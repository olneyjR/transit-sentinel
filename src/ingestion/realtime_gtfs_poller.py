"""
Transit Sentinel - Real-time GTFS Poller

Polls and decodes GTFS-Realtime (GTFS-RT) binary Protocol Buffer feeds.

GTFS-RT is a binary format (not JSON) that provides real-time updates:
- Vehicle Positions: Where buses/trains are right now
- Trip Updates: Schedule deviations and delays
- Service Alerts: Disruptions and notices

This demonstrates senior-level data engineering: working with binary protocols,
not just REST JSON APIs.

Protocol Buffer spec: https://gtfs.org/realtime/
"""

import requests
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict
from pathlib import Path
import time

# Google's GTFS-Realtime protobuf bindings
try:
    from google.transit import gtfs_realtime_pb2
except ImportError:
    raise ImportError(
        "GTFS-Realtime bindings not installed. "
        "Install with: pip install gtfs-realtime-bindings"
    )

from validation.schemas import VehiclePosition, TripUpdate, DataQualityAlert

logger = logging.getLogger(__name__)


class GTFSRealtimePoller:
    """
    Poll and decode GTFS-Realtime Protocol Buffer feeds.
    
    Handles the complexity of:
    1. Binary Protobuf decoding
    2. Data validation against Pydantic schemas
    3. Error handling and retries
    4. Rate limiting
    5. Data quality alerts for invalid records
    """
    
    def __init__(
        self,
        feed_url: str,
        agency_id: str,
        poll_interval_seconds: int = 30,
        timeout_seconds: int = 10,
        max_retries: int = 3
    ):
        """
        Initialize GTFS-RT poller.
        
        Args:
            feed_url: URL to GTFS-RT feed (Protocol Buffer format)
            agency_id: Agency identifier for tracking
            poll_interval_seconds: How often to poll the feed
            timeout_seconds: Request timeout
            max_retries: Number of retry attempts on failure
        """
        self.feed_url = feed_url
        self.agency_id = agency_id
        self.poll_interval = poll_interval_seconds
        self.timeout = timeout_seconds
        self.max_retries = max_retries
        
        self.last_poll_time = 0
        self.last_feed_timestamp = None
        
        # Statistics
        self.stats = {
            'total_polls': 0,
            'successful_polls': 0,
            'failed_polls': 0,
            'total_entities': 0,
            'valid_entities': 0,
            'invalid_entities': 0
        }
        
        logger.info(
            f"Initialized GTFS-RT poller for {agency_id}: {feed_url} "
            f"(poll interval: {poll_interval_seconds}s)"
        )
    
    def _fetch_feed(self) -> bytes:
        """
        Fetch raw binary feed from URL with retries.
        
        Returns:
            Raw bytes of Protocol Buffer message
        """
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Fetching feed from {self.feed_url} (attempt {attempt + 1}/{self.max_retries})")
                
                response = requests.get(
                    self.feed_url,
                    timeout=self.timeout,
                    headers={'Accept': 'application/octet-stream'}
                )
                response.raise_for_status()
                
                logger.debug(f"Received {len(response.content)} bytes")
                
                return response.content
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Feed fetch attempt {attempt + 1} failed: {e}")
                
                if attempt < self.max_retries - 1:
                    # Exponential backoff
                    sleep_time = 2 ** attempt
                    logger.debug(f"Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    raise
    
    def _decode_protobuf(self, raw_data: bytes) -> gtfs_realtime_pb2.FeedMessage:
        """
        Decode binary Protocol Buffer into FeedMessage object.
        
        Args:
            raw_data: Raw bytes from feed
            
        Returns:
            Decoded FeedMessage object
        """
        feed = gtfs_realtime_pb2.FeedMessage()
        
        try:
            feed.ParseFromString(raw_data)
            logger.debug(f"Decoded protobuf: {len(feed.entity)} entities")
            return feed
            
        except Exception as e:
            logger.error(f"Failed to decode protobuf: {e}")
            raise ValueError(f"Invalid protobuf data: {e}")
    
    def _extract_vehicle_position(
        self,
        entity: gtfs_realtime_pb2.FeedEntity,
        feed_timestamp: datetime
    ) -> Optional[VehiclePosition]:
        """
        Extract and validate vehicle position from entity.
        
        Args:
            entity: FeedEntity containing vehicle position
            feed_timestamp: Timestamp of the feed
            
        Returns:
            Validated VehiclePosition or None if invalid
        """
        if not entity.HasField('vehicle'):
            return None
        
        vehicle = entity.vehicle
        
        # Extract required fields
        if not vehicle.HasField('position'):
            logger.debug(f"Entity {entity.id} has no position data")
            return None
        
        position = vehicle.position
        
        # Build dictionary for Pydantic validation
        position_data = {
            'vehicle_id': vehicle.vehicle.id if vehicle.HasField('vehicle') else entity.id,
            'trip_id': vehicle.trip.trip_id if vehicle.HasField('trip') else None,
            'route_id': vehicle.trip.route_id if vehicle.HasField('trip') else None,
            'latitude': position.latitude,
            'longitude': position.longitude,
            'bearing': position.bearing if position.HasField('bearing') else None,
            'speed': position.speed if position.HasField('speed') else None,
            'timestamp': datetime.fromtimestamp(vehicle.timestamp, tz=timezone.utc) if vehicle.HasField('timestamp') else feed_timestamp,
            'current_stop_sequence': vehicle.current_stop_sequence if vehicle.HasField('current_stop_sequence') else None,
            'stop_id': vehicle.stop_id if vehicle.HasField('stop_id') else None,
            'agency_id': self.agency_id,
            'feed_timestamp': feed_timestamp
        }
        
        # Add optional fields with enum mapping
        if vehicle.HasField('current_status'):
            status_map = {
                0: 'INCOMING_AT',
                1: 'STOPPED_AT',
                2: 'IN_TRANSIT_TO'
            }
            position_data['current_status'] = status_map.get(vehicle.current_status)
        
        if vehicle.HasField('congestion_level'):
            congestion_map = {
                0: 'UNKNOWN_CONGESTION_LEVEL',
                1: 'RUNNING_SMOOTHLY',
                2: 'STOP_AND_GO',
                3: 'CONGESTION',
                4: 'SEVERE_CONGESTION'
            }
            position_data['congestion_level'] = congestion_map.get(vehicle.congestion_level)
        
        if vehicle.HasField('occupancy_status'):
            occupancy_map = {
                0: 'EMPTY',
                1: 'MANY_SEATS_AVAILABLE',
                2: 'FEW_SEATS_AVAILABLE',
                3: 'STANDING_ROOM_ONLY',
                4: 'CRUSHED_STANDING_ROOM_ONLY',
                5: 'FULL',
                6: 'NOT_ACCEPTING_PASSENGERS'
            }
            position_data['occupancy_status'] = occupancy_map.get(vehicle.occupancy_status)
        
        try:
            # Validate with Pydantic schema
            validated = VehiclePosition(**position_data)
            return validated
            
        except Exception as e:
            # Validation failed - create data quality alert
            logger.warning(f"Vehicle position validation failed for {position_data.get('vehicle_id')}: {e}")
            
            # Would publish this to data quality alerts topic
            alert = DataQualityAlert(
                alert_id=f"dqa_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{position_data.get('vehicle_id', 'unknown')}",
                alert_type="VALIDATION_ERROR",
                severity="MEDIUM",
                entity_type="vehicle_position",
                entity_id=position_data.get('vehicle_id'),
                agency_id=self.agency_id,
                error_message=str(e)
            )
            
            return None
    
    def _extract_trip_update(
        self,
        entity: gtfs_realtime_pb2.FeedEntity,
        feed_timestamp: datetime
    ) -> List[TripUpdate]:
        """
        Extract and validate trip updates from entity.
        
        A single trip update entity can contain multiple stop time updates.
        
        Args:
            entity: FeedEntity containing trip update
            feed_timestamp: Timestamp of the feed
            
        Returns:
            List of validated TripUpdate objects (one per stop time update)
        """
        if not entity.HasField('trip_update'):
            return []
        
        trip_update = entity.trip_update
        updates = []
        
        # Extract vehicle and trip IDs
        vehicle_id = trip_update.vehicle.id if trip_update.HasField('vehicle') else None
        trip_id = trip_update.trip.trip_id if trip_update.HasField('trip') else None
        route_id = trip_update.trip.route_id if trip_update.HasField('trip') else None
        
        if not trip_id:
            logger.debug(f"Entity {entity.id} has no trip_id")
            return []
        
        # Process each stop time update
        for stu in trip_update.stop_time_update:
            update_data = {
                'trip_id': trip_id,
                'route_id': route_id,
                'vehicle_id': vehicle_id,
                'stop_sequence': stu.stop_sequence if stu.HasField('stop_sequence') else 0,
                'stop_id': stu.stop_id if stu.HasField('stop_id') else '',
                'agency_id': self.agency_id,
                'timestamp': feed_timestamp
            }
            
            # Extract arrival info
            if stu.HasField('arrival'):
                if stu.arrival.HasField('delay'):
                    update_data['arrival_delay'] = stu.arrival.delay
                if stu.arrival.HasField('time'):
                    update_data['arrival_time'] = datetime.fromtimestamp(stu.arrival.time, tz=timezone.utc)
            
            # Extract departure info
            if stu.HasField('departure'):
                if stu.departure.HasField('delay'):
                    update_data['departure_delay'] = stu.departure.delay
                if stu.departure.HasField('time'):
                    update_data['departure_time'] = datetime.fromtimestamp(stu.departure.time, tz=timezone.utc)
            
            # Schedule relationship
            if stu.HasField('schedule_relationship'):
                relationship_map = {
                    0: 'SCHEDULED',
                    1: 'SKIPPED',
                    2: 'NO_DATA',
                    3: 'UNSCHEDULED'
                }
                update_data['schedule_relationship'] = relationship_map.get(stu.schedule_relationship, 'SCHEDULED')
            
            try:
                validated = TripUpdate(**update_data)
                updates.append(validated)
            except Exception as e:
                logger.warning(f"Trip update validation failed: {e}")
                # Could emit data quality alert here
        
        return updates
    
    def poll_once(self) -> Dict[str, List]:
        """
        Perform a single poll of the GTFS-RT feed.
        
        Returns:
            Dict with 'vehicle_positions' and 'trip_updates' lists
        """
        self.stats['total_polls'] += 1
        
        try:
            # Fetch binary feed
            raw_data = self._fetch_feed()
            
            # Decode protobuf
            feed = self._decode_protobuf(raw_data)
            
            # Extract feed timestamp
            feed_timestamp = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
            self.last_feed_timestamp = feed_timestamp
            
            logger.info(
                f"Processing feed from {feed_timestamp.isoformat()} "
                f"with {len(feed.entity)} entities"
            )
            
            # Process entities
            vehicle_positions = []
            trip_updates = []
            
            for entity in feed.entity:
                self.stats['total_entities'] += 1
                
                # Try to extract vehicle position
                if entity.HasField('vehicle'):
                    vp = self._extract_vehicle_position(entity, feed_timestamp)
                    if vp:
                        vehicle_positions.append(vp)
                        self.stats['valid_entities'] += 1
                    else:
                        self.stats['invalid_entities'] += 1
                
                # Try to extract trip updates
                if entity.HasField('trip_update'):
                    tu_list = self._extract_trip_update(entity, feed_timestamp)
                    trip_updates.extend(tu_list)
                    if tu_list:
                        self.stats['valid_entities'] += len(tu_list)
            
            self.stats['successful_polls'] += 1
            self.last_poll_time = time.time()
            
            logger.info(
                f"Poll complete: {len(vehicle_positions)} vehicle positions, "
                f"{len(trip_updates)} trip updates"
            )
            
            return {
                'vehicle_positions': vehicle_positions,
                'trip_updates': trip_updates
            }
            
        except Exception as e:
            self.stats['failed_polls'] += 1
            logger.error(f"Poll failed: {e}", exc_info=True)
            raise
    
    def get_stats(self) -> Dict:
        """Get polling statistics"""
        return {
            **self.stats,
            'success_rate': (
                self.stats['successful_polls'] / self.stats['total_polls']
                if self.stats['total_polls'] > 0 else 0
            ),
            'validation_rate': (
                self.stats['valid_entities'] / self.stats['total_entities']
                if self.stats['total_entities'] > 0 else 0
            )
        }


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== GTFS-Realtime Poller Example ===\n")
    
    # TriMet vehicle positions feed (Portland, OR)
    poller = GTFSRealtimePoller(
        feed_url="https://developer.trimet.org/ws/gtfsrt/VehiclePositions",
        agency_id="trimet",
        poll_interval_seconds=30
    )
    
    try:
        # Perform a single poll
        print("Polling TriMet real-time feed...")
        result = poller.poll_once()
        
        print(f"\n=== Poll Results ===")
        print(f"Vehicle Positions: {len(result['vehicle_positions'])}")
        print(f"Trip Updates: {len(result['trip_updates'])}")
        
        # Display sample vehicle positions
        if result['vehicle_positions']:
            print(f"\n=== Sample Vehicle Positions (first 3) ===")
            for vp in result['vehicle_positions'][:3]:
                print(
                    f"  Vehicle {vp.vehicle_id} (Route {vp.route_id}): "
                    f"({vp.latitude:.4f}, {vp.longitude:.4f}) "
                    f"@ {vp.speed*3.6 if vp.speed else 0:.1f} km/h"
                )
        
        # Display statistics
        print(f"\n=== Statistics ===")
        stats = poller.get_stats()
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2%}")
            else:
                print(f"  {key}: {value}")
        
        print("\n=== Poll Complete ===")
        
    except Exception as e:
        logger.error(f"Example failed: {e}", exc_info=True)