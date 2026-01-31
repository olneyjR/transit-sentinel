"""
Transit Sentinel - Redpanda/Kafka Producer

Publishes validated transit data to Redpanda topics.
Demonstrates event-driven architecture with reliable message delivery.
"""

import json
import logging
from typing import List, Optional, Dict
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class RedpandaProducer:
    """
    Kafka-compatible producer for Redpanda.
    
    Features:
    - Automatic retries with exponential backoff
    - Compression (snappy) for bandwidth efficiency
    - Idempotent writes (acks=all) for reliability
    - JSON serialization with datetime handling
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        compression_type: str = "snappy",
        acks: str = "all"
    ):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            compression_type=compression_type,
            acks=acks,
            retries=3,
            max_in_flight_requests_per_connection=5,
            value_serializer=self._serialize_json
        )
        
        logger.info(f"Initialized Redpanda producer: {bootstrap_servers}")
    
    @staticmethod
    def _serialize_json(data: Dict) -> bytes:
        """JSON serializer with datetime support"""
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        return json.dumps(data, default=json_serial).encode('utf-8')
    
    def publish_vehicle_positions(
        self,
        positions: List,
        topic: str = "transit.vehicle.positions"
    ) -> int:
        """Publish validated vehicle positions"""
        count = 0
        
        for position in positions:
            try:
                # Convert Pydantic model to dict
                data = position.model_dump(mode='json')
                
                # Publish with vehicle_id as key for partitioning
                future = self.producer.send(
                    topic,
                    key=position.vehicle_id.encode('utf-8'),
                    value=data
                )
                
                future.get(timeout=10)  # Block for confirmation
                count += 1
                
            except KafkaError as e:
                logger.error(f"Failed to publish position for {position.vehicle_id}: {e}")
        
        if count > 0:
            logger.info(f"Published {count} vehicle positions to {topic}")
        
        return count
    
    def publish_trip_updates(
        self,
        updates: List,
        topic: str = "transit.trip.updates"
    ) -> int:
        """Publish validated trip updates"""
        count = 0
        
        for update in updates:
            try:
                data = update.model_dump(mode='json')
                
                future = self.producer.send(
                    topic,
                    key=update.trip_id.encode('utf-8'),
                    value=data
                )
                
                future.get(timeout=10)
                count += 1
                
            except KafkaError as e:
                logger.error(f"Failed to publish trip update for {update.trip_id}: {e}")
        
        if count > 0:
            logger.info(f"Published {count} trip updates to {topic}")
        
        return count
    
    def publish_weather(
        self,
        observation,
        topic: str = "transit.weather.observations"
    ) -> bool:
        """Publish weather observation"""
        try:
            data = observation.model_dump(mode='json')
            
            future = self.producer.send(
                topic,
                key=observation.agency_id.encode('utf-8'),
                value=data
            )
            
            future.get(timeout=10)
            logger.info(f"Published weather observation for {observation.agency_id}")
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish weather observation: {e}")
            return False
    
    def close(self):
        """Flush and close producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("Redpanda producer closed")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '..')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Redpanda Producer module - use with run_pipeline.py")