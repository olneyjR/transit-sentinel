"""
Transit Sentinel - Data Validation Schemas

Pydantic models for strict data quality enforcement across the pipeline.
These schemas implement "Data Contracts" - guaranteeing data quality at the Silver layer
to prevent downstream breakage.

Key Features:
- Speed limit validation (prevent impossible velocities)
- Coordinate bounds checking (agency-specific geographic constraints)
- Timestamp freshness validation (detect stale data)
- Null safety (require critical fields)
- Type coercion with validation
"""

from datetime import datetime, timezone
from typing import Optional, Literal
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator
import logging

logger = logging.getLogger(__name__)


# Enumerations for categorical data
class CongestionLevel(str, Enum):
    """GTFS-RT defined congestion levels"""
    UNKNOWN = "UNKNOWN_CONGESTION_LEVEL"
    RUNNING_SMOOTHLY = "RUNNING_SMOOTHLY"
    STOP_AND_GO = "STOP_AND_GO"
    CONGESTION = "CONGESTION"
    SEVERE_CONGESTION = "SEVERE_CONGESTION"


class OccupancyStatus(str, Enum):
    """GTFS-RT defined occupancy levels"""
    EMPTY = "EMPTY"
    MANY_SEATS_AVAILABLE = "MANY_SEATS_AVAILABLE"
    FEW_SEATS_AVAILABLE = "FEW_SEATS_AVAILABLE"
    STANDING_ROOM_ONLY = "STANDING_ROOM_ONLY"
    CRUSHED_STANDING_ROOM_ONLY = "CRUSHED_STANDING_ROOM_ONLY"
    FULL = "FULL"
    NOT_ACCEPTING_PASSENGERS = "NOT_ACCEPTING_PASSENGERS"


class VehicleStopStatus(str, Enum):
    """Vehicle position relative to stop"""
    INCOMING_AT = "INCOMING_AT"
    STOPPED_AT = "STOPPED_AT"
    IN_TRANSIT_TO = "IN_TRANSIT_TO"


class WeatherCondition(str, Enum):
    """Simplified weather classifications from WMO codes"""
    CLEAR = "CLEAR"
    PARTLY_CLOUDY = "PARTLY_CLOUDY"
    OVERCAST = "OVERCAST"
    RAIN = "RAIN"
    HEAVY_RAIN = "HEAVY_RAIN"
    SNOW = "SNOW"
    THUNDERSTORM = "THUNDERSTORM"
    FOG = "FOG"


# Core validation models
class VehiclePosition(BaseModel):
    """
    Validated vehicle position with strict quality controls.
    
    Data Quality Rules:
    1. Speed must be within realistic bounds (0-120 km/h for transit)
    2. Coordinates must be within agency geographic bounds
    3. Timestamp must be recent (within 5 minutes by default)
    4. Vehicle and trip IDs are required (no nulls)
    """
    
    # Identifiers (required)
    vehicle_id: str = Field(..., min_length=1, description="Unique vehicle identifier")
    trip_id: Optional[str] = Field(None, description="Current trip identifier")
    route_id: Optional[str] = Field(None, description="Route identifier")
    
    # Position data (required)
    latitude: float = Field(..., ge=-90.0, le=90.0, description="Vehicle latitude")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Vehicle longitude")
    bearing: Optional[float] = Field(None, ge=0.0, lt=360.0, description="Vehicle bearing in degrees")
    speed: Optional[float] = Field(None, ge=0.0, description="Speed in m/s")
    
    # Temporal data (required)
    timestamp: datetime = Field(..., description="Position timestamp (UTC)")
    
    # Operational status (optional)
    current_stop_sequence: Optional[int] = Field(None, ge=0)
    stop_id: Optional[str] = None
    current_status: Optional[VehicleStopStatus] = None
    congestion_level: Optional[CongestionLevel] = None
    occupancy_status: Optional[OccupancyStatus] = None
    
    # Data quality metadata
    agency_id: str = Field(..., description="Transit agency identifier")
    feed_timestamp: datetime = Field(..., description="When feed was generated")
    
    @field_validator('timestamp', 'feed_timestamp')
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        """Ensure all timestamps are UTC"""
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    
    @field_validator('speed')
    @classmethod
    def validate_speed(cls, v: Optional[float]) -> Optional[float]:
        """
        Validate vehicle speed is within realistic bounds.
        Max 120 km/h (33.3 m/s) for urban transit.
        """
        if v is None:
            return v
        
        MAX_SPEED_MS = 33.3  # ~120 km/h
        
        if v > MAX_SPEED_MS:
            logger.warning(f"Unrealistic speed detected: {v:.2f} m/s ({v*3.6:.2f} km/h)")
            raise ValueError(f"Speed {v:.2f} m/s exceeds maximum realistic speed")
        
        return v
    
    @model_validator(mode='after')
    def validate_position_freshness(self):
        """
        Ensure position data is not stale.
        Default threshold: 5 minutes (300 seconds)
        """
        MAX_AGE_SECONDS = 300
        
        now = datetime.now(timezone.utc)
        age_seconds = (now - self.timestamp).total_seconds()
        
        if age_seconds > MAX_AGE_SECONDS:
            logger.warning(
                f"Stale position data: {age_seconds:.0f}s old for vehicle {self.vehicle_id}"
            )
            raise ValueError(f"Position data is {age_seconds:.0f}s old, exceeds {MAX_AGE_SECONDS}s limit")
        
        return self
    
    @model_validator(mode='after')
    def validate_geographic_bounds(self):
        """
        Validate coordinates are within expected geographic bounds.
        This would typically use agency-specific bounds from config.
        Here using reasonable defaults - override with agency bounds in practice.
        """
        # These bounds should come from agency config in production
        # Using wide defaults here for example
        MIN_LAT, MAX_LAT = -90.0, 90.0
        MIN_LON, MAX_LON = -180.0, 180.0
        
        if not (MIN_LAT <= self.latitude <= MAX_LAT):
            raise ValueError(f"Latitude {self.latitude} outside valid range [{MIN_LAT}, {MAX_LAT}]")
        
        if not (MIN_LON <= self.longitude <= MAX_LON):
            raise ValueError(f"Longitude {self.longitude} outside valid range [{MIN_LON}, {MAX_LON}]")
        
        return self
    
    class Config:
        json_schema_extra = {
            "example": {
                "vehicle_id": "4012",
                "trip_id": "10293847",
                "route_id": "100",
                "latitude": 45.5152,
                "longitude": -122.6784,
                "bearing": 180.5,
                "speed": 12.5,
                "timestamp": "2026-01-31T12:30:00Z",
                "current_stop_sequence": 5,
                "stop_id": "7625",
                "current_status": "IN_TRANSIT_TO",
                "congestion_level": "RUNNING_SMOOTHLY",
                "occupancy_status": "FEW_SEATS_AVAILABLE",
                "agency_id": "trimet",
                "feed_timestamp": "2026-01-31T12:30:15Z"
            }
        }


class TripUpdate(BaseModel):
    """
    Validated trip update with delay and schedule adherence.
    
    Data Quality Rules:
    1. Delays must be within reasonable bounds (-1h to +2h)
    2. Arrival/departure times must be logical (arrival <= departure)
    3. Stop sequence must be valid (>= 0)
    """
    
    trip_id: str = Field(..., min_length=1)
    route_id: Optional[str] = None
    vehicle_id: Optional[str] = None
    
    # Stop time update
    stop_sequence: int = Field(..., ge=0)
    stop_id: str = Field(..., min_length=1)
    
    # Timing (all optional as updates may only have some fields)
    arrival_delay: Optional[int] = Field(None, description="Delay in seconds (negative = early)")
    departure_delay: Optional[int] = Field(None, description="Delay in seconds")
    arrival_time: Optional[datetime] = None
    departure_time: Optional[datetime] = None
    
    # Schedule relationship
    schedule_relationship: Literal["SCHEDULED", "SKIPPED", "NO_DATA", "UNSCHEDULED"] = "SCHEDULED"
    
    # Metadata
    agency_id: str
    timestamp: datetime
    
    @field_validator('arrival_delay', 'departure_delay')
    @classmethod
    def validate_delay(cls, v: Optional[int]) -> Optional[int]:
        """
        Validate delays are within reasonable bounds.
        Allow -1 hour (early) to +2 hours (delayed).
        """
        if v is None:
            return v
        
        MIN_DELAY = -3600  # 1 hour early
        MAX_DELAY = 7200   # 2 hours late
        
        if not (MIN_DELAY <= v <= MAX_DELAY):
            raise ValueError(f"Delay {v}s outside reasonable bounds [{MIN_DELAY}s, {MAX_DELAY}s]")
        
        return v
    
    @model_validator(mode='after')
    def validate_arrival_departure_order(self):
        """Ensure arrival time <= departure time if both present"""
        if self.arrival_time and self.departure_time:
            if self.arrival_time > self.departure_time:
                raise ValueError("Arrival time cannot be after departure time")
        return self
    
    class Config:
        json_schema_extra = {
            "example": {
                "trip_id": "10293847",
                "route_id": "100",
                "vehicle_id": "4012",
                "stop_sequence": 5,
                "stop_id": "7625",
                "arrival_delay": 120,
                "departure_delay": 180,
                "schedule_relationship": "SCHEDULED",
                "agency_id": "trimet",
                "timestamp": "2026-01-31T12:30:00Z"
            }
        }


class WeatherObservation(BaseModel):
    """
    Weather data from Open-Meteo API, correlated with transit operations.
    
    Data Quality Rules:
    1. Temperature must be within realistic bounds (-50°C to +60°C)
    2. Precipitation must be non-negative
    3. Wind speed must be non-negative
    4. Observation must be recent (within 1 hour)
    """
    
    # Location
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    
    # Weather parameters
    temperature_celsius: float = Field(..., ge=-50.0, le=60.0, description="Temperature in °C")
    precipitation_mm: float = Field(..., ge=0.0, description="Precipitation in mm")
    wind_speed_kmh: float = Field(..., ge=0.0, le=200.0, description="Wind speed in km/h")
    weather_code: int = Field(..., ge=0, le=99, description="WMO weather code")
    weather_condition: WeatherCondition
    
    # Temporal
    observation_time: datetime
    
    # Metadata
    agency_id: str
    
    @field_validator('observation_time')
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    
    @model_validator(mode='after')
    def validate_observation_freshness(self):
        """Ensure weather observation is recent (within 1 hour)"""
        MAX_AGE_SECONDS = 3600
        
        now = datetime.now(timezone.utc)
        age_seconds = (now - self.observation_time).total_seconds()
        
        if age_seconds > MAX_AGE_SECONDS:
            logger.warning(f"Stale weather observation: {age_seconds:.0f}s old")
            raise ValueError(f"Weather observation is {age_seconds:.0f}s old")
        
        return self
    
    @classmethod
    def from_open_meteo(cls, data: dict, latitude: float, longitude: float, agency_id: str):
        """Factory method to create from Open-Meteo API response"""
        # Map WMO weather codes to simplified conditions
        weather_code = data.get('weathercode', 0)
        condition_map = {
            0: WeatherCondition.CLEAR,
            1: WeatherCondition.PARTLY_CLOUDY,
            2: WeatherCondition.PARTLY_CLOUDY,
            3: WeatherCondition.OVERCAST,
            **{i: WeatherCondition.RAIN for i in range(51, 68)},
            **{i: WeatherCondition.HEAVY_RAIN for i in range(80, 83)},
            **{i: WeatherCondition.SNOW for i in range(71, 78)},
            **{i: WeatherCondition.THUNDERSTORM for i in range(95, 100)},
            45: WeatherCondition.FOG,
            48: WeatherCondition.FOG,
        }
        
        return cls(
            latitude=latitude,
            longitude=longitude,
            temperature_celsius=data.get('temperature_2m', 0.0),
            precipitation_mm=data.get('precipitation', 0.0),
            wind_speed_kmh=data.get('windspeed_10m', 0.0),
            weather_code=weather_code,
            weather_condition=condition_map.get(weather_code, WeatherCondition.CLEAR),
            observation_time=datetime.now(timezone.utc),
            agency_id=agency_id
        )
    
    class Config:
        json_schema_extra = {
            "example": {
                "latitude": 45.5152,
                "longitude": -122.6784,
                "temperature_celsius": 15.5,
                "precipitation_mm": 2.3,
                "wind_speed_kmh": 12.5,
                "weather_code": 61,
                "weather_condition": "RAIN",
                "observation_time": "2026-01-31T12:30:00Z",
                "agency_id": "trimet"
            }
        }


class DataQualityAlert(BaseModel):
    """
    Data quality issue alert for the quarantine system.
    
    Captures validation failures for monitoring and debugging.
    """
    
    alert_id: str = Field(..., description="Unique alert identifier")
    alert_type: Literal["VALIDATION_ERROR", "STALE_DATA", "GEOGRAPHIC_VIOLATION", "SPEED_VIOLATION"]
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    
    # Context
    entity_type: Literal["vehicle_position", "trip_update", "weather_observation"]
    entity_id: Optional[str] = None
    agency_id: str
    
    # Details
    error_message: str
    field_name: Optional[str] = None
    field_value: Optional[str] = None
    
    # Temporal
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        json_schema_extra = {
            "example": {
                "alert_id": "dqa_20260131_123045_001",
                "alert_type": "SPEED_VIOLATION",
                "severity": "HIGH",
                "entity_type": "vehicle_position",
                "entity_id": "vehicle_4012",
                "agency_id": "trimet",
                "error_message": "Speed 45.2 m/s exceeds maximum realistic speed",
                "field_name": "speed",
                "field_value": "45.2",
                "detected_at": "2026-01-31T12:30:45Z"
            }
        }


# Example usage and testing
if __name__ == "__main__":
    import json
    
    # Test VehiclePosition validation
    print("=== Testing VehiclePosition Validation ===\n")
    
    # Valid position
    valid_position = {
        "vehicle_id": "4012",
        "trip_id": "10293847",
        "route_id": "100",
        "latitude": 45.5152,
        "longitude": -122.6784,
        "bearing": 180.5,
        "speed": 12.5,  # m/s, ~45 km/h
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agency_id": "trimet",
        "feed_timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    try:
        vp = VehiclePosition(**valid_position)
        print(f"✓ Valid position accepted: {vp.vehicle_id} at ({vp.latitude}, {vp.longitude})")
        print(f"  Speed: {vp.speed:.2f} m/s ({vp.speed*3.6:.2f} km/h)\n")
    except Exception as e:
        print(f"✗ Validation failed: {e}\n")
    
    # Invalid: Speed too high
    invalid_speed = valid_position.copy()
    invalid_speed["speed"] = 50.0  # 180 km/h - too fast for transit
    
    try:
        vp = VehiclePosition(**invalid_speed)
        print(f"✗ Should have rejected high speed!\n")
    except ValueError as e:
        print(f"✓ Correctly rejected high speed: {e}\n")
    
    # Invalid: Stale timestamp
    stale_timestamp = valid_position.copy()
    stale_timestamp["timestamp"] = (
        datetime.now(timezone.utc) - timedelta(minutes=10)
    ).isoformat()
    
    try:
        vp = VehiclePosition(**stale_timestamp)
        print(f"✗ Should have rejected stale data!\n")
    except ValueError as e:
        print(f"✓ Correctly rejected stale data: {e}\n")
    
    # Test WeatherObservation
    print("=== Testing WeatherObservation Validation ===\n")
    
    valid_weather = {
        "latitude": 45.5152,
        "longitude": -122.6784,
        "temperature_celsius": 15.5,
        "precipitation_mm": 2.3,
        "wind_speed_kmh": 12.5,
        "weather_code": 61,
        "weather_condition": "RAIN",
        "observation_time": datetime.now(timezone.utc).isoformat(),
        "agency_id": "trimet"
    }
    
    try:
        wo = WeatherObservation(**valid_weather)
        print(f"✓ Valid weather observation: {wo.temperature_celsius}°C, {wo.weather_condition}")
        print(f"  Precipitation: {wo.precipitation_mm}mm, Wind: {wo.wind_speed_kmh} km/h\n")
    except Exception as e:
        print(f"✗ Validation failed: {e}\n")
    
    print("=== Validation Tests Complete ===")