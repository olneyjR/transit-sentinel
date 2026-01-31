"""
Transit Sentinel - Weather Enrichment

Fetch weather data from Open-Meteo API and correlate with vehicle positions.

This demonstrates a key 2026 data engineering pattern: enriching operational data
with environmental context for impact analysis.

Use cases:
- Correlate delays with weather conditions
- Identify weather-sensitive routes
- Predict service disruptions
- Optimize fleet deployment during adverse conditions

Open-Meteo: https://open-meteo.com/
- Free, no API key required
- Hourly forecasts and historical data
- Global coverage
"""

import requests
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List
import time

from validation.schemas import WeatherObservation

logger = logging.getLogger(__name__)


class WeatherEnrichment:
    """
    Fetch and correlate weather data with transit operations.
    
    Uses Open-Meteo API to get current weather conditions and forecasts
    for transit agency service areas.
    """
    
    def __init__(
        self,
        api_base: str = "https://api.open-meteo.com/v1",
        timeout_seconds: int = 10
    ):
        """
        Initialize weather enrichment.
        
        Args:
            api_base: Open-Meteo API base URL
            timeout_seconds: Request timeout
        """
        self.api_base = api_base
        self.timeout = timeout_seconds
        
        # Cache to avoid redundant API calls
        self._cache: Dict[str, tuple[datetime, Dict]] = {}
        self._cache_ttl_seconds = 300  # 5 minutes
        
        logger.info("Initialized Weather Enrichment with Open-Meteo API")
    
    def _get_cache_key(self, latitude: float, longitude: float) -> str:
        """Generate cache key from coordinates (rounded to 2 decimals)"""
        return f"{latitude:.2f},{longitude:.2f}"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still fresh"""
        if cache_key not in self._cache:
            return False
        
        cached_time, _ = self._cache[cache_key]
        age_seconds = (datetime.now(timezone.utc) - cached_time).total_seconds()
        
        return age_seconds < self._cache_ttl_seconds
    
    def fetch_current_weather(
        self,
        latitude: float,
        longitude: float,
        agency_id: str,
        use_cache: bool = True
    ) -> Optional[WeatherObservation]:
        """
        Fetch current weather conditions for a location.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            agency_id: Agency identifier for tracking
            use_cache: Use cached data if available
            
        Returns:
            Validated WeatherObservation or None if fetch fails
        """
        cache_key = self._get_cache_key(latitude, longitude)
        
        # Check cache first
        if use_cache and self._is_cache_valid(cache_key):
            logger.debug(f"Using cached weather for {cache_key}")
            _, cached_data = self._cache[cache_key]
            
            try:
                return WeatherObservation.from_open_meteo(
                    data=cached_data,
                    latitude=latitude,
                    longitude=longitude,
                    agency_id=agency_id
                )
            except Exception as e:
                logger.warning(f"Failed to use cached weather data: {e}")
                # Fall through to fresh fetch
        
        # Fetch fresh data
        try:
            logger.debug(f"Fetching current weather for ({latitude:.4f}, {longitude:.4f})")
            
            response = requests.get(
                f"{self.api_base}/forecast",
                params={
                    'latitude': latitude,
                    'longitude': longitude,
                    'current_weather': 'true',
                    'temperature_unit': 'celsius',
                    'windspeed_unit': 'kmh',
                    'precipitation_unit': 'mm'
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            current = data.get('current_weather', {})
            
            if not current:
                logger.warning(f"No current weather data in response for {cache_key}")
                return None
            
            # Update cache
            self._cache[cache_key] = (datetime.now(timezone.utc), current)
            
            # Create validated observation
            observation = WeatherObservation.from_open_meteo(
                data=current,
                latitude=latitude,
                longitude=longitude,
                agency_id=agency_id
            )
            
            logger.info(
                f"Weather at ({latitude:.2f}, {longitude:.2f}): "
                f"{observation.temperature_celsius}°C, {observation.weather_condition}"
            )
            
            return observation
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather data: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing weather data: {e}", exc_info=True)
            return None
    
    def fetch_hourly_forecast(
        self,
        latitude: float,
        longitude: float,
        hours_ahead: int = 24
    ) -> Optional[List[Dict]]:
        """
        Fetch hourly weather forecast.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            hours_ahead: Number of hours to forecast (max 168 = 7 days)
            
        Returns:
            List of hourly forecast dictionaries
        """
        try:
            logger.debug(f"Fetching {hours_ahead}h forecast for ({latitude:.4f}, {longitude:.4f})")
            
            response = requests.get(
                f"{self.api_base}/forecast",
                params={
                    'latitude': latitude,
                    'longitude': longitude,
                    'hourly': 'temperature_2m,precipitation,windspeed_10m,weathercode',
                    'temperature_unit': 'celsius',
                    'windspeed_unit': 'kmh',
                    'precipitation_unit': 'mm',
                    'forecast_days': min(hours_ahead // 24 + 1, 7)  # Max 7 days
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            hourly = data.get('hourly', {})
            
            if not hourly:
                logger.warning("No hourly data in forecast response")
                return None
            
            # Parse hourly data into list of dicts
            times = hourly.get('time', [])
            temps = hourly.get('temperature_2m', [])
            precip = hourly.get('precipitation', [])
            wind = hourly.get('windspeed_10m', [])
            weather_codes = hourly.get('weathercode', [])
            
            forecast = []
            for i, time_str in enumerate(times[:hours_ahead]):
                forecast.append({
                    'time': datetime.fromisoformat(time_str.replace('Z', '+00:00')),
                    'temperature_celsius': temps[i] if i < len(temps) else None,
                    'precipitation_mm': precip[i] if i < len(precip) else None,
                    'wind_speed_kmh': wind[i] if i < len(wind) else None,
                    'weather_code': weather_codes[i] if i < len(weather_codes) else None
                })
            
            logger.info(f"Retrieved {len(forecast)} hourly forecasts")
            
            return forecast
            
        except Exception as e:
            logger.error(f"Failed to fetch hourly forecast: {e}", exc_info=True)
            return None
    
    def correlate_with_vehicles(
        self,
        vehicle_positions: List,
        weather_center_lat: float,
        weather_center_lon: float,
        agency_id: str
    ) -> Optional[WeatherObservation]:
        """
        Get weather for a fleet of vehicles (using agency center point).
        
        For simplicity, we use a single weather observation at the agency's
        center point rather than individual observations per vehicle.
        
        In a production system, you might:
        1. Grid the service area
        2. Fetch weather for each grid cell
        3. Spatially join vehicles to nearest grid cell
        
        Args:
            vehicle_positions: List of VehiclePosition objects
            weather_center_lat: Center latitude for weather observation
            weather_center_lon: Center longitude for weather observation
            agency_id: Agency identifier
            
        Returns:
            WeatherObservation for the service area
        """
        if not vehicle_positions:
            logger.warning("No vehicle positions provided for correlation")
            return None
        
        logger.info(
            f"Correlating weather with {len(vehicle_positions)} vehicles "
            f"using center point ({weather_center_lat:.2f}, {weather_center_lon:.2f})"
        )
        
        weather = self.fetch_current_weather(
            latitude=weather_center_lat,
            longitude=weather_center_lon,
            agency_id=agency_id
        )
        
        if weather:
            # Log weather impact analysis
            severe_conditions = weather.weather_condition in ['HEAVY_RAIN', 'SNOW', 'THUNDERSTORM']
            
            if severe_conditions:
                logger.warning(
                    f"Severe weather detected: {weather.weather_condition} "
                    f"({weather.temperature_celsius}°C, {weather.precipitation_mm}mm precip)"
                )
            else:
                logger.info(
                    f"Normal weather conditions: {weather.weather_condition} "
                    f"({weather.temperature_celsius}°C)"
                )
        
        return weather
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        valid_entries = sum(
            1 for key in self._cache
            if self._is_cache_valid(key)
        )
        
        return {
            'total_entries': len(self._cache),
            'valid_entries': valid_entries,
            'cache_ttl_seconds': self._cache_ttl_seconds,
            'cache_hit_rate': valid_entries / len(self._cache) if self._cache else 0
        }


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== Weather Enrichment Example ===\n")
    
    weather_service = WeatherEnrichment()
    
    # Example: Portland, OR (TriMet service area)
    portland_lat = 45.5152
    portland_lon = -122.6784
    
    try:
        # Fetch current weather
        print(f"Fetching current weather for Portland, OR...")
        observation = weather_service.fetch_current_weather(
            latitude=portland_lat,
            longitude=portland_lon,
            agency_id="trimet"
        )
        
        if observation:
            print(f"\n=== Current Weather ===")
            print(f"Location: ({observation.latitude}, {observation.longitude})")
            print(f"Temperature: {observation.temperature_celsius}°C")
            print(f"Precipitation: {observation.precipitation_mm}mm")
            print(f"Wind Speed: {observation.wind_speed_kmh} km/h")
            print(f"Condition: {observation.weather_condition}")
            print(f"WMO Code: {observation.weather_code}")
            print(f"Observed at: {observation.observation_time.isoformat()}")
        
        # Fetch hourly forecast
        print(f"\nFetching 24-hour forecast...")
        forecast = weather_service.fetch_hourly_forecast(
            latitude=portland_lat,
            longitude=portland_lon,
            hours_ahead=24
        )
        
        if forecast:
            print(f"\n=== 24-Hour Forecast (first 6 hours) ===")
            for hour in forecast[:6]:
                print(
                    f"{hour['time'].strftime('%H:%M')}: "
                    f"{hour['temperature_celsius']}°C, "
                    f"{hour['precipitation_mm']}mm precip, "
                    f"{hour['wind_speed_kmh']} km/h wind"
                )
        
        # Cache statistics
        print(f"\n=== Cache Statistics ===")
        stats = weather_service.get_cache_stats()
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2%}")
            else:
                print(f"{key}: {value}")
        
        print("\n=== Weather Enrichment Complete ===")
        
    except Exception as e:
        logger.error(f"Example failed: {e}", exc_info=True)