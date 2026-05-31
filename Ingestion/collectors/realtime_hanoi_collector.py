"""
Real-time optimized collector for Hanoi air quality and weather data
Targets sub-minute latency with high throughput streaming architecture
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import aioredis
from kafka import KafkaProducer
from kafka.errors import KafkaError
import numpy as np
import pandas as pd

# Hanoi specific coordinates and stations
HANOI_STATIONS = [
    {"id": "hanoi_center", "lat": 21.0285, "lon": 105.8542, "name": "Hanoi Center"},
    {"id": "hanoi_cau_giay", "lat": 21.0333, "lon": 105.7969, "name": "Cau Giay"},
    {"id": "hanoi_dong_da", "lat": 21.0136, "lon": 105.8372, "name": "Dong Da"},
    {"id": "hanoi_hai_ba_trung", "lat": 21.0058, "lon": 105.8581, "name": "Hai Ba Trung"},
]

@dataclass
class RealtimeRecord:
    """Optimized data structure for real-time processing"""
    station_id: str
    timestamp: datetime
    source: str
    data_type: str  # 'weather' or 'air_quality'
    
    # Air quality fields
    pm25: Optional[float] = None
    pm10: Optional[float] = None
    aqi: Optional[int] = None
    no2: Optional[float] = None
    o3: Optional[float] = None
    co: Optional[float] = None
    so2: Optional[float] = None
    
    # Weather fields
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    pressure: Optional[float] = None
    wind_speed: Optional[float] = None
    wind_direction: Optional[float] = None
    
    # Metadata
    latency_ms: Optional[int] = None
    quality_score: Optional[float] = None

class CircuitBreaker:
    """Circuit breaker pattern for API resilience"""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"

class RealtimeHanoiCollector:
    """High-performance real-time collector optimized for Hanoi region"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Performance settings
        self.poll_interval = config.get('poll_interval', 30)  # 30 seconds for real-time
        self.batch_size = config.get('batch_size', 10)
        self.max_concurrent = config.get('max_concurrent', 6)
        
        # Initialize components
        self.redis_client = None
        self.kafka_producer = None
        self.session = None
        self.circuit_breakers = {}
        
        # Metrics
        self.metrics = {
            'records_processed': 0,
            'api_calls': 0,
            'errors': 0,
            'avg_latency_ms': 0,
            'last_update': None
        }
        
        # API configurations with fallbacks
        self.api_configs = {
            'openmeteo': {
                'weather_url': 'https://api.open-meteo.com/v1/forecast',
                'air_url': 'https://air-quality-api.open-meteo.com/v1/air-quality',
                'timeout': 5,
                'priority': 1
            },
            'openweathermap': {
                'weather_url': 'https://api.openweathermap.org/data/2.5/weather',
                'air_url': 'https://api.openweathermap.org/data/2.5/air_pollution',
                'timeout': 8,
                'priority': 2,
                'api_key': config.get('openweathermap_api_key')
            },
            'iqair': {
                'url': 'https://api.airvisual.com/v2/nearest_city',
                'timeout': 10,
                'priority': 3,
                'api_key': config.get('iqair_api_key')
            }
        }
    
    async def initialize(self):
        """Initialize async components"""
        # Redis for high-performance checkpointing
        self.redis_client = aioredis.from_url(
            self.config.get('redis_url', 'redis://localhost:6379'),
            decode_responses=True
        )
        
        # Kafka producer with optimized settings
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=self.config.get('kafka_servers', ['localhost:9092']),
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            compression_type='lz4',
            linger_ms=50,  # Batch for 50ms for better throughput
            batch_size=65536,  # 64KB batches
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=5
        )
        
        # HTTP session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=20,
            limit_per_host=10,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=15)
        )
        
        # Initialize circuit breakers
        for api_name in self.api_configs:
            self.circuit_breakers[api_name] = CircuitBreaker()
        
        self.logger.info("RealtimeHanoiCollector initialized")
    
    async def fetch_weather_data(self, station: Dict, api_name: str) -> Optional[RealtimeRecord]:
        """Fetch weather data with circuit breaker protection"""
        if not self.circuit_breakers[api_name].can_execute():
            return None
        
        start_time = time.time()
        
        try:
            if api_name == 'openmeteo':
                return await self._fetch_openmeteo_weather(station, start_time)
            elif api_name == 'openweathermap':
                return await self._fetch_openweathermap_weather(station, start_time)
            
        except Exception as e:
            self.circuit_breakers[api_name].record_failure()
            self.logger.error(f"Weather API {api_name} failed for {station['id']}: {e}")
            self.metrics['errors'] += 1
            return None
    
    async def _fetch_openmeteo_weather(self, station: Dict, start_time: float) -> Optional[RealtimeRecord]:
        """Optimized Open-Meteo weather fetching"""
        params = {
            'latitude': station['lat'],
            'longitude': station['lon'],
            'current': 'temperature_2m,relative_humidity_2m,pressure_msl,wind_speed_10m,wind_direction_10m',
            'timezone': 'GMT'
        }
        
        async with self.session.get(
            self.api_configs['openmeteo']['weather_url'],
            params=params,
            timeout=aiohttp.ClientTimeout(total=self.api_configs['openmeteo']['timeout'])
        ) as response:
            if response.status == 200:
                data = await response.json()
                current = data.get('current', {})
                
                latency_ms = int((time.time() - start_time) * 1000)
                
                record = RealtimeRecord(
                    station_id=station['id'],
                    timestamp=datetime.now(timezone.utc),
                    source='openmeteo',
                    data_type='weather',
                    temperature=current.get('temperature_2m'),
                    humidity=current.get('relative_humidity_2m'),
                    pressure=current.get('pressure_msl'),
                    wind_speed=current.get('wind_speed_10m'),
                    wind_direction=current.get('wind_direction_10m'),
                    latency_ms=latency_ms,
                    quality_score=self._calculate_quality_score(current, 'weather')
                )
                
                self.circuit_breakers['openmeteo'].record_success()
                return record
            else:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status
                )
    
    async def fetch_air_quality_data(self, station: Dict, api_name: str) -> Optional[RealtimeRecord]:
        """Fetch air quality data with circuit breaker protection"""
        if not self.circuit_breakers[api_name].can_execute():
            return None
        
        start_time = time.time()
        
        try:
            if api_name == 'openmeteo':
                return await self._fetch_openmeteo_air(station, start_time)
            elif api_name == 'iqair':
                return await self._fetch_iqair_data(station, start_time)
            
        except Exception as e:
            self.circuit_breakers[api_name].record_failure()
            self.logger.error(f"Air quality API {api_name} failed for {station['id']}: {e}")
            self.metrics['errors'] += 1
            return None
    
    async def _fetch_openmeteo_air(self, station: Dict, start_time: float) -> Optional[RealtimeRecord]:
        """Optimized Open-Meteo air quality fetching"""
        params = {
            'latitude': station['lat'],
            'longitude': station['lon'],
            'current': 'pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,us_aqi',
            'timezone': 'GMT'
        }
        
        async with self.session.get(
            self.api_configs['openmeteo']['air_url'],
            params=params,
            timeout=aiohttp.ClientTimeout(total=self.api_configs['openmeteo']['timeout'])
        ) as response:
            if response.status == 200:
                data = await response.json()
                current = data.get('current', {})
                
                latency_ms = int((time.time() - start_time) * 1000)
                
                record = RealtimeRecord(
                    station_id=station['id'],
                    timestamp=datetime.now(timezone.utc),
                    source='openmeteo',
                    data_type='air_quality',
                    pm25=current.get('pm2_5'),
                    pm10=current.get('pm10'),
                    aqi=current.get('us_aqi'),
                    co=current.get('carbon_monoxide'),
                    no2=current.get('nitrogen_dioxide'),
                    so2=current.get('sulphur_dioxide'),
                    o3=current.get('ozone'),
                    latency_ms=latency_ms,
                    quality_score=self._calculate_quality_score(current, 'air_quality')
                )
                
                self.circuit_breakers['openmeteo'].record_success()
                return record
            else:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status
                )
    
    def _calculate_quality_score(self, data: Dict, data_type: str) -> float:
        """Calculate data quality score (0-1)"""
        if data_type == 'weather':
            required_fields = ['temperature_2m', 'relative_humidity_2m', 'pressure_msl']
        else:  # air_quality
            required_fields = ['pm2_5', 'pm10', 'us_aqi']
        
        present_fields = sum(1 for field in required_fields if data.get(field) is not None)
        return present_fields / len(required_fields)
    
    async def collect_station_data(self, station: Dict) -> List[RealtimeRecord]:
        """Collect all data for a single station with fallback logic"""
        records = []
        
        # Collect weather data with fallback
        weather_record = None
        for api_name in sorted(self.api_configs.keys(), key=lambda x: self.api_configs[x]['priority']):
            if 'weather_url' in self.api_configs[api_name]:
                weather_record = await self.fetch_weather_data(station, api_name)
                if weather_record:
                    break
        
        if weather_record:
            records.append(weather_record)
        
        # Collect air quality data with fallback
        air_record = None
        for api_name in sorted(self.api_configs.keys(), key=lambda x: self.api_configs[x]['priority']):
            if 'air_url' in self.api_configs[api_name] or api_name == 'iqair':
                air_record = await self.fetch_air_quality_data(station, api_name)
                if air_record:
                    break
        
        if air_record:
            records.append(air_record)
        
        return records
    
    async def publish_to_kafka(self, records: List[RealtimeRecord]):
        """Publish records to Kafka with error handling"""
        for record in records:
            try:
                topic = f"hanoi_{record.data_type}_realtime"
                message = asdict(record)
                
                # Add processing metadata
                message['processing_time'] = datetime.now(timezone.utc).isoformat()
                message['collector_version'] = '2.0.0'
                
                future = self.kafka_producer.send(topic, value=message)
                
                # Non-blocking callback
                future.add_callback(self._on_send_success)
                future.add_errback(self._on_send_error)
                
            except Exception as e:
                self.logger.error(f"Failed to publish record: {e}")
                self.metrics['errors'] += 1
    
    def _on_send_success(self, record_metadata):
        """Kafka send success callback"""
        self.metrics['records_processed'] += 1
    
    def _on_send_error(self, excp):
        """Kafka send error callback"""
        self.logger.error(f"Kafka send failed: {excp}")
        self.metrics['errors'] += 1
    
    async def update_checkpoint(self, station_id: str, timestamp: datetime):
        """Update checkpoint in Redis for fast access"""
        checkpoint_key = f"checkpoint:hanoi:{station_id}"
        checkpoint_data = {
            'last_processed': timestamp.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        await self.redis_client.hset(checkpoint_key, mapping=checkpoint_data)
        await self.redis_client.expire(checkpoint_key, 86400)  # 24 hour TTL
    
    async def get_last_checkpoint(self, station_id: str) -> Optional[datetime]:
        """Get last checkpoint from Redis"""
        checkpoint_key = f"checkpoint:hanoi:{station_id}"
        last_processed = await self.redis_client.hget(checkpoint_key, 'last_processed')
        
        if last_processed:
            return datetime.fromisoformat(last_processed)
        return None
    
    async def collect_realtime_data(self):
        """Main collection loop with concurrent processing"""
        self.logger.info("Starting real-time data collection for Hanoi")
        
        while True:
            cycle_start = time.time()
            
            try:
                # Collect data from all stations concurrently
                tasks = []
                for station in HANOI_STATIONS:
                    task = asyncio.create_task(self.collect_station_data(station))
                    tasks.append((station, task))
                
                # Wait for all tasks with timeout
                all_records = []
                for station, task in tasks:
                    try:
                        records = await asyncio.wait_for(task, timeout=20)
                        all_records.extend(records)
                        
                        # Update checkpoint for successful collection
                        if records:
                            await self.update_checkpoint(station['id'], records[0].timestamp)
                        
                    except asyncio.TimeoutError:
                        self.logger.warning(f"Timeout collecting data for station {station['id']}")
                        self.metrics['errors'] += 1
                
                # Publish all records
                if all_records:
                    await self.publish_to_kafka(all_records)
                
                # Update metrics
                cycle_time = time.time() - cycle_start
                self.metrics['avg_latency_ms'] = int(cycle_time * 1000)
                self.metrics['last_update'] = datetime.now(timezone.utc)
                self.metrics['api_calls'] += len(HANOI_STATIONS) * 2  # weather + air for each station
                
                # Log performance metrics
                if self.metrics['records_processed'] % 100 == 0:
                    self.logger.info(f"Performance: {self.metrics}")
                
                # Adaptive sleep to maintain target interval
                sleep_time = max(0, self.poll_interval - cycle_time)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    self.logger.warning(f"Collection cycle took {cycle_time:.2f}s, exceeding target {self.poll_interval}s")
                
            except Exception as e:
                self.logger.error(f"Error in collection cycle: {e}")
                self.metrics['errors'] += 1
                await asyncio.sleep(5)  # Brief pause on error
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.redis_client:
            await self.redis_client.close()
        
        self.logger.info("RealtimeHanoiCollector cleaned up")

async def main():
    """Main entry point"""
    config = {
        'poll_interval': 30,  # 30 seconds for real-time
        'redis_url': 'redis://localhost:6379',
        'kafka_servers': ['localhost:9092'],
        'openweathermap_api_key': 'your_api_key_here',
        'iqair_api_key': 'your_api_key_here'
    }
    
    collector = RealtimeHanoiCollector(config)
    
    try:
        await collector.initialize()
        await collector.collect_realtime_data()
    except KeyboardInterrupt:
        logging.info("Shutting down collector...")
    finally:
        await collector.cleanup()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())