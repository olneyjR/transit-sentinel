#!/usr/bin/env python3
"""
Transit Sentinel - Main Pipeline Runner

Continuous operation mode that:
1. Polls GTFS-RT feeds every 30 seconds
2. Fetches weather every 5 minutes
3. Validates and streams data through Redpanda
4. Loads into DuckDB with Medallion Architecture
5. Aggregates metrics to Gold layer

Run with: python run_pipeline.py
Stop with: Ctrl+C
"""

import sys
import time
import signal
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from ingestion.realtime_gtfs_poller import GTFSRealtimePoller
from ingestion.weather_enrichment import WeatherEnrichment
from ingestion.static_gtfs_fetcher import StaticGTFSFetcher
from streaming.redpanda_producer import RedpandaProducer
from analytics.duckdb_loader import DuckDBLoader


class TransitSentinelPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """Initialize pipeline with configuration"""
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        with open("config/agency_config.yaml", 'r') as f:
            agency_config = yaml.safe_load(f)
        
        # Get default agency
        agency_id = agency_config['default_agency']
        self.agency = agency_config['agencies'][agency_id]
        self.agency_id = agency_id
        
        # Setup logging
        self._setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing Transit Sentinel for {self.agency['name']}")
        
        # Initialize components
        self._init_components()
        
        # Runtime state
        self.running = False
        self.last_weather_fetch = 0
        self.poll_count = 0
        
    def _setup_logging(self):
        """Configure logging"""
        log_config = self.config['logging']
        log_dir = Path(log_config['log_dir'])
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format=log_config['format'],
            handlers=[
                logging.FileHandler(log_dir / 'transit-sentinel.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _init_components(self):
        """Initialize pipeline components"""
        
        # GTFS-RT Poller
        self.poller = GTFSRealtimePoller(
            feed_url=self.agency['feeds']['vehicle_positions'],
            agency_id=self.agency_id,
            poll_interval_seconds=self.config['polling']['gtfs_realtime_seconds']
        )
        
        # Weather service
        self.weather_service = WeatherEnrichment(
            api_base=self.config['apis']['open_meteo']['base_url']
        )
        
        # DuckDB loader
        self.db_loader = DuckDBLoader(
            database_path=self.config['duckdb']['database_path']
        )
        
        # Redpanda producer (optional - comment out if not using streaming)
        try:
            self.producer = RedpandaProducer(
                bootstrap_servers=self.config['redpanda']['bootstrap_servers']
            )
            self.use_streaming = True
        except Exception as e:
            self.logger.warning(f"Redpanda not available, using direct DB load: {e}")
            self.producer = None
            self.use_streaming = False
        
        self.logger.info("Pipeline components initialized")
    
    def _should_fetch_weather(self) -> bool:
        """Check if it's time to fetch weather"""
        weather_interval = self.config['polling']['weather_seconds']
        elapsed = time.time() - self.last_weather_fetch
        return elapsed >= weather_interval
    
    def process_iteration(self):
        """Process one iteration of the pipeline"""
        
        iteration_start = time.time()
        self.poll_count += 1
        
        self.logger.info(f"=== Iteration {self.poll_count} ===")
        
        try:
            # 1. Poll GTFS-RT feed
            self.logger.info("Polling GTFS-RT feed...")
            result = self.poller.poll_once()
            
            vehicle_positions = result['vehicle_positions']
            trip_updates = result['trip_updates']
            
            self.logger.info(
                f"Polled: {len(vehicle_positions)} vehicles, "
                f"{len(trip_updates)} trip updates"
            )
            
            # 2. Fetch weather if needed
            weather = None
            if self._should_fetch_weather():
                self.logger.info("Fetching weather...")
                
                weather = self.weather_service.fetch_current_weather(
                    latitude=self.agency['weather_center']['latitude'],
                    longitude=self.agency['weather_center']['longitude'],
                    agency_id=self.agency_id
                )
                
                self.last_weather_fetch = time.time()
                
                if weather:
                    self.logger.info(
                        f"Weather: {weather.temperature_celsius}°C, "
                        f"{weather.weather_condition}"
                    )
            
            # 3. Stream to Redpanda (if available)
            if self.use_streaming and vehicle_positions:
                self.logger.info("Publishing to Redpanda...")
                self.producer.publish_vehicle_positions(vehicle_positions)
                
                if trip_updates:
                    self.producer.publish_trip_updates(trip_updates)
                
                if weather:
                    self.producer.publish_weather(weather)
            
            # 4. Load to DuckDB
            if vehicle_positions:
                self.logger.info("Loading to DuckDB Bronze...")
                self.db_loader.load_vehicle_positions_bronze(vehicle_positions)
                
                # Promote to Silver
                self.db_loader.promote_to_silver()
                
                # Aggregate to Gold
                if self.poll_count % 10 == 0:  # Every 10 iterations
                    self.logger.info("Aggregating to Gold layer...")
                    self.db_loader.aggregate_to_gold_hourly()
            
            # 5. Report metrics
            if self.poll_count % 10 == 0:
                metrics = self.db_loader.get_current_metrics()
                self.logger.info(f"Metrics: {metrics['silver_vehicle_positions']:,} records in Silver")
                
                stats = self.poller.get_stats()
                self.logger.info(
                    f"Poller stats: {stats['success_rate']:.1%} success rate, "
                    f"{stats['validation_rate']:.1%} validation rate"
                )
            
            # Calculate sleep time
            iteration_time = time.time() - iteration_start
            sleep_time = max(
                0,
                self.config['polling']['gtfs_realtime_seconds'] - iteration_time
            )
            
            if sleep_time > 0:
                self.logger.debug(f"Sleeping {sleep_time:.1f}s until next iteration")
                time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.error(f"Iteration failed: {e}", exc_info=True)
    
    def run(self):
        """Run pipeline continuously"""
        
        self.logger.info("Starting Transit Sentinel pipeline...")
        self.logger.info(f"Agency: {self.agency['name']}")
        self.logger.info(f"Poll interval: {self.config['polling']['gtfs_realtime_seconds']}s")
        
        self.running = True
        
        try:
            while self.running:
                self.process_iteration()
        
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal...")
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        
        self.logger.info("Shutting down Transit Sentinel...")
        
        self.running = False
        
        # Close connections
        if self.producer:
            self.producer.close()
        
        self.db_loader.close()
        
        # Print final stats
        self.logger.info("=== Final Statistics ===")
        self.logger.info(f"Total iterations: {self.poll_count}")
        
        stats = self.poller.get_stats()
        self.logger.info(f"Total polls: {stats['total_polls']}")
        self.logger.info(f"Successful: {stats['successful_polls']} ({stats['success_rate']:.1%})")
        self.logger.info(f"Total entities: {stats['total_entities']}")
        self.logger.info(f"Valid entities: {stats['valid_entities']} ({stats['validation_rate']:.1%})")
        
        metrics = self.db_loader.get_current_metrics()
        self.logger.info(f"Bronze records: {metrics['bronze_vehicle_positions']:,}")
        self.logger.info(f"Silver records: {metrics['silver_vehicle_positions']:,}")
        self.logger.info(f"Gold metrics: {metrics['gold_hourly_metrics']}")
        
        self.logger.info("Transit Sentinel stopped")


def main():
    """Main entry point"""
    
    print("=" * 70)
    print("  TRANSIT SENTINEL - REAL-TIME GEOSPATIAL INTELLIGENCE")
    print("=" * 70)
    print()
    print("Starting pipeline in continuous mode...")
    print("Press Ctrl+C to stop")
    print()
    
    # Handle signals for graceful shutdown
    pipeline: Optional[TransitSentinelPipeline] = None
    
    def signal_handler(signum, frame):
        if pipeline:
            pipeline.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        pipeline = TransitSentinelPipeline()
        pipeline.run()
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logging.exception("Detailed error:")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())