"""
High-performance Kafka stream processor for real-time Hanoi air quality data
Implements low-latency stream processing with windowing and aggregations
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import statistics
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import aioredis
import pandas as pd

@dataclass
class ProcessedRecord:
    """Processed real-time record with enrichments"""
    station_id: str
    timestamp: datetime
    data_type: str
    
    # Original measurements
    raw_data: Dict[str, Any]
    
    # Real-time features
    pm25_trend_5min: Optional[float] = None
    pm25_trend_15min: Optional[float] = None
    aqi_change_rate: Optional[float] = None
    
    # Weather correlations
    temp_pm25_correlation: Optional[float] = None
    wind_dispersion_factor: Optional[float] = None
    
    # Quality indicators
    data_completeness: float = 0.0
    anomaly_score: Optional[float] = None
    
    # Processing metadata
    processing_latency_ms: int = 0
    window_size: int = 0

class TimeWindow:
    """Sliding time window for real-time aggregations"""
    def __init__(self, window_size_minutes: int, max_records: int = 1000):
        self.window_size = timedelta(minutes=window_size_minutes)
        self.max_records = max_records
        self.records = deque(maxlen=max_records)
    
    def add_record(self, record: Dict[str, Any]):
        """Add record with timestamp-based cleanup"""
        now = datetime.now(timezone.utc)
        cutoff_time = now - self.window_size
        
        # Remove old records
        while self.records and self.records[0]['timestamp'] < cutoff_time:
            self.records.popleft()
        
        # Add new record
        self.records.append({
            'timestamp': now,
            'data': record
        })
    
    def get_values(self, field: str) -> List[float]:
        """Extract numeric values for a field"""
        values = []
        for record in self.records:
            value = record['data'].get(field)
            if value is not None and isinstance(value, (int, float)):
                values.append(float(value))
        return values
    
    def calculate_trend(self, field: str) -> Optional[float]:
        """Calculate linear trend (slope) for a field"""
        values = self.get_values(field)
        if len(values) < 2:
            return None
        
        # Simple linear regression slope
        n = len(values)
        x = list(range(n))
        
        x_mean = statistics.mean(x)
        y_mean = statistics.mean(values)
        
        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator
    
    def size(self) -> int:
        return len(self.records)

class AnomalyDetector:
    """Real-time anomaly detection using statistical methods"""
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.history = defaultdict(lambda: deque(maxlen=window_size))
    
    def detect_anomaly(self, station_id: str, field: str, value: float) -> float:
        """Detect anomaly using z-score method, returns anomaly score (0-1)"""
        history = self.history[f"{station_id}_{field}"]
        
        if len(history) < 10:  # Need minimum history
            history.append(value)
            return 0.0
        
        # Calculate z-score
        mean_val = statistics.mean(history)
        std_val = statistics.stdev(history) if len(history) > 1 else 1.0
        
        if std_val == 0:
            z_score = 0
        else:
            z_score = abs(value - mean_val) / std_val
        
        # Convert z-score to 0-1 anomaly score
        anomaly_score = min(z_score / 3.0, 1.0)  # 3-sigma rule
        
        # Update history
        history.append(value)
        
        return anomaly_score

class KafkaStreamProcessor:
    """High-performance stream processor for Hanoi air quality data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Processing components
        self.windows = {
            '5min': defaultdict(lambda: TimeWindow(5)),
            '15min': defaultdict(lambda: TimeWindow(15)),
            '60min': defaultdict(lambda: TimeWindow(60))
        }
        self.anomaly_detector = AnomalyDetector()
        
        # Kafka components
        self.consumer = None
        self.producer = None
        self.redis_client = None
        
        # Performance metrics
        self.metrics = {
            'messages_processed': 0,
            'processing_errors': 0,
            'avg_processing_time_ms': 0,
            'anomalies_detected': 0,
            'last_processed': None
        }
        
        # Processing configuration
        self.batch_size = config.get('batch_size', 100)
        self.processing_timeout = config.get('processing_timeout', 5.0)
    
    async def initialize(self):
        """Initialize stream processor components"""
        # Kafka consumer with optimized settings
        self.consumer = KafkaConsumer(
            'hanoi_weather_realtime',
            'hanoi_air_quality_realtime',
            bootstrap_servers=self.config.get('kafka_servers', ['localhost:9092']),
            group_id='hanoi_stream_processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            max_poll_records=self.batch_size,
            fetch_min_bytes=1024,
            fetch_max_wait_ms=500
        )
        
        # Kafka producer for processed data
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.get('kafka_servers', ['localhost:9092']),
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            compression_type='lz4',
            linger_ms=100,  # Batch for 100ms
            batch_size=65536,
            acks='1',  # Leader acknowledgment for speed
            retries=2
        )
        
        # Redis for caching and state
        self.redis_client = aioredis.from_url(
            self.config.get('redis_url', 'redis://localhost:6379'),
            decode_responses=True
        )
        
        self.logger.info("KafkaStreamProcessor initialized")
    
    def process_record(self, raw_record: Dict[str, Any]) -> ProcessedRecord:
        """Process a single record with real-time enrichments"""
        start_time = datetime.now()
        
        station_id = raw_record.get('station_id')
        data_type = raw_record.get('data_type')
        timestamp = datetime.fromisoformat(raw_record.get('timestamp'))
        
        # Add to time windows
        for window_name, windows in self.windows.items():
            windows[station_id].add_record(raw_record)
        
        # Calculate real-time features
        processed = ProcessedRecord(
            station_id=station_id,
            timestamp=timestamp,
            data_type=data_type,
            raw_data=raw_record
        )
        
        # PM2.5 trend analysis
        if data_type == 'air_quality' and raw_record.get('pm25') is not None:
            processed.pm25_trend_5min = self.windows['5min'][station_id].calculate_trend('pm25')
            processed.pm25_trend_15min = self.windows['15min'][station_id].calculate_trend('pm25')
            
            # AQI change rate
            aqi_values = self.windows['5min'][station_id].get_values('aqi')
            if len(aqi_values) >= 2:
                processed.aqi_change_rate = aqi_values[-1] - aqi_values[-2]
        
        # Weather correlations
        if data_type == 'weather':
            processed.wind_dispersion_factor = self._calculate_wind_dispersion(raw_record)
        
        # Cross-correlation between weather and air quality
        processed.temp_pm25_correlation = self._calculate_temp_pm25_correlation(station_id)
        
        # Data quality assessment
        processed.data_completeness = self._calculate_completeness(raw_record, data_type)
        
        # Anomaly detection
        if data_type == 'air_quality' and raw_record.get('pm25') is not None:
            processed.anomaly_score = self.anomaly_detector.detect_anomaly(
                station_id, 'pm25', raw_record['pm25']
            )
            
            if processed.anomaly_score > 0.7:  # High anomaly threshold
                self.metrics['anomalies_detected'] += 1
                self.logger.warning(f"Anomaly detected: {station_id} PM2.5={raw_record['pm25']} score={processed.anomaly_score:.3f}")
        
        # Processing metadata
        processing_time = datetime.now() - start_time
        processed.processing_latency_ms = int(processing_time.total_seconds() * 1000)
        processed.window_size = self.windows['5min'][station_id].size()
        
        return processed
    
    def _calculate_wind_dispersion(self, weather_record: Dict[str, Any]) -> Optional[float]:
        """Calculate wind dispersion factor for pollutant transport"""
        wind_speed = weather_record.get('wind_speed')
        if wind_speed is None:
            return None
        
        # Simple dispersion model: higher wind speed = better dispersion
        # Normalize to 0-1 scale where 1 = excellent dispersion
        return min(wind_speed / 10.0, 1.0)  # 10 m/s = perfect dispersion
    
    def _calculate_temp_pm25_correlation(self, station_id: str) -> Optional[float]:
        """Calculate correlation between temperature and PM2.5"""
        temp_values = []
        pm25_values = []
        
        # Get recent weather and air quality data
        for record in self.windows['15min'][station_id].records:
            data = record['data']
            if data.get('data_type') == 'weather' and data.get('temperature') is not None:
                temp_values.append(data['temperature'])
            elif data.get('data_type') == 'air_quality' and data.get('pm25') is not None:
                pm25_values.append(data['pm25'])
        
        if len(temp_values) < 3 or len(pm25_values) < 3:
            return None
        
        # Align arrays (take minimum length)
        min_len = min(len(temp_values), len(pm25_values))
        temp_values = temp_values[-min_len:]
        pm25_values = pm25_values[-min_len:]
        
        # Calculate Pearson correlation
        try:
            correlation = np.corrcoef(temp_values, pm25_values)[0, 1]
            return correlation if not np.isnan(correlation) else None
        except:
            return None
    
    def _calculate_completeness(self, record: Dict[str, Any], data_type: str) -> float:
        """Calculate data completeness score"""
        if data_type == 'weather':
            required_fields = ['temperature', 'humidity', 'pressure', 'wind_speed']
        else:  # air_quality
            required_fields = ['pm25', 'pm10', 'aqi']
        
        present_count = sum(1 for field in required_fields if record.get(field) is not None)
        return present_count / len(required_fields)
    
    async def publish_processed_record(self, processed_record: ProcessedRecord):
        """Publish processed record to output topics"""
        try:
            # Main processed stream
            main_topic = f"hanoi_{processed_record.data_type}_processed"
            message = asdict(processed_record)
            
            future = self.producer.send(main_topic, value=message)
            
            # High-priority alerts for anomalies
            if processed_record.anomaly_score and processed_record.anomaly_score > 0.8:
                alert_topic = "hanoi_air_quality_alerts"
                alert_message = {
                    'alert_type': 'anomaly',
                    'station_id': processed_record.station_id,
                    'timestamp': processed_record.timestamp.isoformat(),
                    'anomaly_score': processed_record.anomaly_score,
                    'pm25_value': processed_record.raw_data.get('pm25'),
                    'severity': 'high' if processed_record.anomaly_score > 0.9 else 'medium'
                }
                self.producer.send(alert_topic, value=alert_message)
            
            # Cache latest processed data in Redis
            await self._cache_latest_data(processed_record)
            
        except Exception as e:
            self.logger.error(f"Failed to publish processed record: {e}")
            self.metrics['processing_errors'] += 1
    
    async def _cache_latest_data(self, processed_record: ProcessedRecord):
        """Cache latest processed data in Redis for fast access"""
        cache_key = f"latest:{processed_record.station_id}:{processed_record.data_type}"
        cache_data = {
            'timestamp': processed_record.timestamp.isoformat(),
            'pm25_trend_5min': processed_record.pm25_trend_5min,
            'pm25_trend_15min': processed_record.pm25_trend_15min,
            'anomaly_score': processed_record.anomaly_score,
            'data_completeness': processed_record.data_completeness,
            'processing_latency_ms': processed_record.processing_latency_ms
        }
        
        # Remove None values
        cache_data = {k: v for k, v in cache_data.items() if v is not None}
        
        await self.redis_client.hset(cache_key, mapping=cache_data)
        await self.redis_client.expire(cache_key, 3600)  # 1 hour TTL
    
    async def process_stream(self):
        """Main stream processing loop"""
        self.logger.info("Starting Kafka stream processing for Hanoi")
        
        while True:
            try:
                # Poll for messages with timeout
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                
                if not message_batch:
                    continue
                
                batch_start = datetime.now()
                processed_count = 0
                
                # Process messages in batch
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Process individual record
                            processed_record = self.process_record(message.value)
                            
                            # Publish processed record
                            await self.publish_processed_record(processed_record)
                            
                            processed_count += 1
                            self.metrics['messages_processed'] += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                            self.metrics['processing_errors'] += 1
                
                # Update performance metrics
                if processed_count > 0:
                    batch_time = datetime.now() - batch_start
                    avg_time_ms = int((batch_time.total_seconds() * 1000) / processed_count)
                    self.metrics['avg_processing_time_ms'] = avg_time_ms
                    self.metrics['last_processed'] = datetime.now(timezone.utc)
                    
                    # Log performance every 1000 messages
                    if self.metrics['messages_processed'] % 1000 == 0:
                        self.logger.info(f"Stream processing metrics: {self.metrics}")
                
                # Commit offsets
                self.consumer.commit()
                
            except Exception as e:
                self.logger.error(f"Error in stream processing loop: {e}")
                await asyncio.sleep(1)
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        if self.redis_client:
            await self.redis_client.close()
        
        self.logger.info("KafkaStreamProcessor cleaned up")

async def main():
    """Main entry point"""
    config = {
        'kafka_servers': ['localhost:9092'],
        'redis_url': 'redis://localhost:6379',
        'batch_size': 50,
        'processing_timeout': 5.0
    }
    
    processor = KafkaStreamProcessor(config)
    
    try:
        await processor.initialize()
        await processor.process_stream()
    except KeyboardInterrupt:
        logging.info("Shutting down stream processor...")
    finally:
        await processor.cleanup()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())