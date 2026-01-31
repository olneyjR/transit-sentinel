"""
Transit Sentinel - Redpanda/Kafka Consumer

Consumes validated transit data for analytics loading.
"""

import json
import logging
from typing import Callable, Dict, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class RedpandaConsumer:
    """Kafka-compatible consumer for Redpanda"""
    
    def __init__(
        self,
        topics: List[str],
        group_id: str = "transit-sentinel-consumer",
        bootstrap_servers: str = "localhost:9092",
        auto_offset_reset: str = "latest"
    ):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        logger.info(f"Initialized consumer for topics: {topics}")
    
    def consume_batch(
        self,
        max_records: int = 500,
        timeout_ms: int = 1000
    ) -> List[Dict]:
        """Consume a batch of records"""
        records = []
        
        messages = self.consumer.poll(
            timeout_ms=timeout_ms,
            max_records=max_records
        )
        
        for topic_partition, msgs in messages.items():
            for msg in msgs:
                records.append({
                    'topic': topic_partition.topic,
                    'partition': topic_partition.partition,
                    'offset': msg.offset,
                    'timestamp': msg.timestamp,
                    'key': msg.key.decode('utf-8') if msg.key else None,
                    'value': msg.value
                })
        
        if records:
            logger.info(f"Consumed {len(records)} records")
        
        return records
    
    def close(self):
        """Close consumer"""
        self.consumer.close()
        logger.info("Redpanda consumer closed")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Redpanda Consumer module - use with run_pipeline.py")