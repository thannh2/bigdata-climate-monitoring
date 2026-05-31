"""
WebSocket server for real-time Hanoi air quality data streaming
Provides low-latency data feeds for web applications and dashboards
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Set, Optional, Any
import websockets
from websockets.server import WebSocketServerProtocol
import aioredis
from kafka import KafkaConsumer
import threading
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class ClientSubscription:
    """Client subscription configuration"""
    websocket: WebSocketServerProtocol
    station_ids: Set[str]
    data_types: Set[str]  # 'weather', 'air_quality', 'alerts'
    update_interval: int = 5  # seconds
    last_update: Optional[datetime] = None

class WebSocketStreamer:
    """High-performance WebSocket server for real-time data streaming"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Client management
        self.clients: Dict[str, ClientSubscription] = {}
        self.client_counter = 0
        
        # Data cache
        self.redis_client = None
        self.latest_data = defaultdict(dict)
        
        # Kafka consumer for real-time updates
        self.kafka_consumer = None
        self.kafka_thread = None
        self.running = False
        
        # Performance metrics
        self.metrics = {
            'connected_clients': 0,
            'messages_sent': 0,
            'data_updates': 0,
            'errors': 0,
            'uptime_start': datetime.now(timezone.utc)
        }
    
    async def initialize(self):
        """Initialize WebSocket server components"""
        # Redis for caching latest data
        self.redis_client = aioredis.from_url(
            self.config.get('redis_url', 'redis://localhost:6379'),
            decode_responses=True
        )
        
        # Start Kafka consumer in background thread
        self.running = True
        self.kafka_thread = threading.Thread(target=self._kafka_consumer_loop, daemon=True)
        self.kafka_thread.start()
        
        self.logger.info("WebSocketStreamer initialized")
    
    def _kafka_consumer_loop(self):
        """Background Kafka consumer for real-time data updates"""
        try:
            self.kafka_consumer = KafkaConsumer(
                'hanoi_weather_processed',
                'hanoi_air_quality_processed',
                'hanoi_air_quality_alerts',
                bootstrap_servers=self.config.get('kafka_servers', ['localhost:9092']),
                group_id='websocket_streamer',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )
            
            while self.running:
                try:
                    message_batch = self.kafka_consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self._process_kafka_message(message.topic, message.value)
                            
                except Exception as e:
                    self.logger.error(f"Error in Kafka consumer loop: {e}")
                    
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
    
    def _process_kafka_message(self, topic: str, data: Dict[str, Any]):
        """Process incoming Kafka message and update cache"""
        try:
            station_id = data.get('station_id')
            if not station_id:
                return
            
            # Determine data type from topic
            if 'weather' in topic:
                data_type = 'weather'
            elif 'air_quality' in topic and 'alerts' not in topic:
                data_type = 'air_quality'
            elif 'alerts' in topic:
                data_type = 'alerts'
            else:
                return
            
            # Update local cache
            self.latest_data[station_id][data_type] = {
                'timestamp': data.get('timestamp'),
                'data': data,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            self.metrics['data_updates'] += 1
            
            # Trigger client updates asynchronously
            asyncio.create_task(self._broadcast_update(station_id, data_type, data))
            
        except Exception as e:
            self.logger.error(f"Error processing Kafka message: {e}")
            self.metrics['errors'] += 1
    
    async def _broadcast_update(self, station_id: str, data_type: str, data: Dict[str, Any]):
        """Broadcast update to subscribed clients"""
        current_time = datetime.now(timezone.utc)
        disconnected_clients = []
        
        for client_id, subscription in self.clients.items():
            try:
                # Check if client is subscribed to this update
                if (station_id in subscription.station_ids and 
                    data_type in subscription.data_types):
                    
                    # Check update interval
                    if (subscription.last_update is None or 
                        (current_time - subscription.last_update).total_seconds() >= subscription.update_interval):
                        
                        # Prepare message
                        message = {
                            'type': 'data_update',
                            'station_id': station_id,
                            'data_type': data_type,
                            'timestamp': current_time.isoformat(),
                            'data': data
                        }
                        
                        # Send to client
                        await subscription.websocket.send(json.dumps(message))
                        subscription.last_update = current_time
                        self.metrics['messages_sent'] += 1
                        
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.append(client_id)
            except Exception as e:
                self.logger.error(f"Error broadcasting to client {client_id}: {e}")
                disconnected_clients.append(client_id)
        
        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self._remove_client(client_id)
    
    async def handle_client_connection(self, websocket: WebSocketServerProtocol, path: str):
        """Handle new WebSocket client connection"""
        client_id = f"client_{self.client_counter}"
        self.client_counter += 1
        
        self.logger.info(f"New client connected: {client_id} from {websocket.remote_address}")
        
        try:
            # Send welcome message
            welcome_message = {
                'type': 'connection_established',
                'client_id': client_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'available_stations': list(self.latest_data.keys()),
                'available_data_types': ['weather', 'air_quality', 'alerts']
            }
            await websocket.send(json.dumps(welcome_message))
            
            # Handle client messages
            async for message in websocket:
                await self._handle_client_message(client_id, websocket, message)
                
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Client {client_id} disconnected")
        except Exception as e:
            self.logger.error(f"Error handling client {client_id}: {e}")
        finally:
            self._remove_client(client_id)
    
    async def _handle_client_message(self, client_id: str, websocket: WebSocketServerProtocol, message: str):
        """Handle incoming client message"""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            
            if message_type == 'subscribe':
                await self._handle_subscription(client_id, websocket, data)
            elif message_type == 'unsubscribe':
                await self._handle_unsubscription(client_id, data)
            elif message_type == 'get_latest':
                await self._handle_get_latest(client_id, websocket, data)
            elif message_type == 'ping':
                await self._handle_ping(websocket)
            else:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Unknown message type: {message_type}'
                }))
                
        except json.JSONDecodeError:
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Invalid JSON message'
            }))
        except Exception as e:
            self.logger.error(f"Error handling message from {client_id}: {e}")
            await websocket.send(json.dumps({
                'type': 'error',
                'message': 'Internal server error'
            }))
    
    async def _handle_subscription(self, client_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle client subscription request"""
        station_ids = set(data.get('station_ids', []))
        data_types = set(data.get('data_types', ['weather', 'air_quality']))
        update_interval = data.get('update_interval', 5)
        
        # Validate inputs
        valid_stations = set(self.latest_data.keys())
        valid_data_types = {'weather', 'air_quality', 'alerts'}
        
        station_ids = station_ids.intersection(valid_stations) if station_ids else valid_stations
        data_types = data_types.intersection(valid_data_types)
        update_interval = max(1, min(update_interval, 60))  # 1-60 seconds
        
        # Create or update subscription
        self.clients[client_id] = ClientSubscription(
            websocket=websocket,
            station_ids=station_ids,
            data_types=data_types,
            update_interval=update_interval
        )
        
        self.metrics['connected_clients'] = len(self.clients)
        
        # Send confirmation
        response = {
            'type': 'subscription_confirmed',
            'client_id': client_id,
            'station_ids': list(station_ids),
            'data_types': list(data_types),
            'update_interval': update_interval,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await websocket.send(json.dumps(response))
        
        # Send initial data
        await self._send_initial_data(client_id, websocket)
    
    async def _send_initial_data(self, client_id: str, websocket: WebSocketServerProtocol):
        """Send initial data to newly subscribed client"""
        subscription = self.clients.get(client_id)
        if not subscription:
            return
        
        for station_id in subscription.station_ids:
            station_data = self.latest_data.get(station_id, {})
            
            for data_type in subscription.data_types:
                if data_type in station_data:
                    message = {
                        'type': 'initial_data',
                        'station_id': station_id,
                        'data_type': data_type,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'data': station_data[data_type]['data']
                    }
                    await websocket.send(json.dumps(message))
    
    async def _handle_get_latest(self, client_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle request for latest data"""
        station_id = data.get('station_id')
        data_type = data.get('data_type')
        
        if station_id and data_type:
            # Get specific data
            station_data = self.latest_data.get(station_id, {})
            if data_type in station_data:
                response = {
                    'type': 'latest_data',
                    'station_id': station_id,
                    'data_type': data_type,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': station_data[data_type]['data']
                }
            else:
                response = {
                    'type': 'error',
                    'message': f'No data available for {station_id}/{data_type}'
                }
        else:
            # Get all available data
            response = {
                'type': 'all_latest_data',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'data': dict(self.latest_data)
            }
        
        await websocket.send(json.dumps(response))
    
    async def _handle_ping(self, websocket: WebSocketServerProtocol):
        """Handle ping message"""
        pong_message = {
            'type': 'pong',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'server_metrics': self.metrics.copy()
        }
        await websocket.send(json.dumps(pong_message))
    
    def _remove_client(self, client_id: str):
        """Remove disconnected client"""
        if client_id in self.clients:
            del self.clients[client_id]
            self.metrics['connected_clients'] = len(self.clients)
            self.logger.info(f"Removed client {client_id}")
    
    async def start_server(self):
        """Start WebSocket server"""
        host = self.config.get('host', 'localhost')
        port = self.config.get('port', 8765)
        
        self.logger.info(f"Starting WebSocket server on {host}:{port}")
        
        async with websockets.serve(
            self.handle_client_connection,
            host,
            port,
            ping_interval=30,
            ping_timeout=10,
            max_size=1024*1024,  # 1MB max message size
            compression=None  # Disable compression for lower latency
        ):
            self.logger.info(f"WebSocket server running on ws://{host}:{port}")
            
            # Keep server running
            while self.running:
                await asyncio.sleep(1)
    
    async def cleanup(self):
        """Cleanup resources"""
        self.running = False
        
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        if self.kafka_thread and self.kafka_thread.is_alive():
            self.kafka_thread.join(timeout=5)
        
        if self.redis_client:
            await self.redis_client.close()
        
        # Close all client connections
        for client_id, subscription in list(self.clients.items()):
            try:
                await subscription.websocket.close()
            except:
                pass
        
        self.clients.clear()
        self.logger.info("WebSocketStreamer cleaned up")

async def main():
    """Main entry point"""
    config = {
        'host': '0.0.0.0',
        'port': 8765,
        'kafka_servers': ['localhost:9092'],
        'redis_url': 'redis://localhost:6379'
    }
    
    streamer = WebSocketStreamer(config)
    
    try:
        await streamer.initialize()
        await streamer.start_server()
    except KeyboardInterrupt:
        logging.info("Shutting down WebSocket server...")
    finally:
        await streamer.cleanup()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())