# ==============================================
# Mazda to MQTT Bridge for Home Assistant
# ==============================================

# CHANGELOG:
# 2025-01-16 12:44:20 -06:00 - Major code cleanup
# - Removed deprecated functions and classes
# - Simplified monitoring and state management
# - Improved error handling
# - Reduced code duplication

import os
import sys
import json
import time
import asyncio
import logging
import aiohttp
from datetime import datetime
from typing import Dict, Any, List, Optional
from paho.mqtt import client as mqtt_client
import paho.mqtt.client as mqtt
from pymazda.client import Client as MazdaAPI
from pymazda.exceptions import MazdaException
from dotenv import load_dotenv

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================
# Constants and Configuration
# ==============================================

# Update Intervals Configuration (in seconds)
# ==============================================

UPDATE_INTERVALS = {
    # --------------------------------------
    # Operation Mode Settings
    # --------------------------------------
    'BATCH_MODE': True,        # Should cars be updated in groups? 
                            # FALSE: Update one car at a time (more frequent updates, gentler on API)
                            # TRUE:  Update cars in groups (faster overall, but might hit API limits)
    
    'BATCH_SIZE': 3,          # How many cars to update at once when BATCH_MODE is TRUE
                            # Example: With 19 cars and BATCH_SIZE = 3
                            # - First 3 cars update together
                            # - Wait BATCH_INTERVAL seconds
                            # - Next 3 cars update together
                            # - And so on...
                            # Recommended: Start with 3, increase if stable
                            # Maximum: 19 (not recommended - likely to hit rate limits)
    
    # --------------------------------------
    # Phase Control Settings
    # --------------------------------------
    'ENABLE_STATUS_UPDATES': True,  # Enable/disable status updates (doors, windows, etc.)
                                # Set to False to temporarily disable status updates
                                # Useful for debugging or reducing API load
    
    'ENABLE_HEALTH_UPDATES': True,  # Enable/disable health updates (tire pressure, oil life, etc.)
                                # Set to False if health updates are causing API issues
                                # Can run with only status updates if needed
    
    # --------------------------------------
    # Phase Timing Settings
    # --------------------------------------
    'STATUS_DELAY': 5,          # Seconds between each car's status update
                                # Lower = Faster updates but more API load
                                # Recommended: 5-10 seconds for stability
    
    'HEALTH_DELAY': 15,         # Seconds between each car's health update
                                # Higher = More gentle on API but slower updates
                                # Recommended: 15-30 seconds to avoid disconnects
    
    'PHASE_SWITCH_DELAY': 30,      # Seconds to wait between status and health phases
                                    # Gives API time to recover between phases
                                    # Recommended: 30-60 seconds for stability
    
    # --------------------------------------
    # Original Timing Settings
    # --------------------------------------
    'INDIVIDUAL_UPDATE': 10,    # How long to wait between updating each car in sequential mode
                                # Only used when BATCH_MODE = FALSE
                                # Lower = More frequent updates but more API calls
                                # Example: 10 seconds = each car updates every 10 seconds
                                # Recommended: 10-30 seconds to avoid rate limits
    
    'STATUS_HEALTH_DELAY': 10,  # How long to wait between getting status and health for SAME car
                                # Applies in both batch and sequential modes
                                # Helps prevent rate limits by spacing out API calls
                                # Example: Get car status → wait 5 seconds → get car health
                                # Recommended: Keep at 5 seconds unless you have issues
    
    'BATCH_UPDATE': 300,       # How often to run a complete batch update cycle
                              # Only used when BATCH_MODE = TRUE
                              # Example with BATCH_SIZE = 3:
                              # - Update all cars in groups of 3
                              # - Wait this long before starting over
                              # Recommended: 300 seconds (5 minutes) or higher
    
    'BATCH_INTERVAL': 25,      # How long to wait between each batch of cars
                              # Only used when BATCH_MODE = TRUE
                              # Example: Update 3 cars → wait 30s → update next 3 cars
                              # Recommended: 30 seconds to avoid rate limits
    
    # --------------------------------------
    # Error Handling Settings
    # --------------------------------------
    'ERROR_RETRY': 30,          # How long to wait after an error before trying again
                                # Applies to any API error (rate limits, connection issues, etc)
                                # Gives the API time to recover before retrying
                                # Recommended: Leave at 30 seconds
    
    'MAX_RETRIES': 3,           # How many times to retry after an error
                                # After this many failures, will wait for next update cycle
                                # Recommended: Leave at 3
    
    'RETRY_BACKOFF': 2          # How much to increase wait time between retries
                                # Example: First retry = 30s, Second = 60s, Third = 120s
                                # Recommended: Leave at 2
}

# API Settings
API_SETTINGS = {
    'TIMEOUT': 30,              # Timeout for individual API calls
    'RETRY_DELAY': 10,          # Delay between API call retries
    'MAX_RETRIES': 3,           # Maximum number of retry attempts
    'VEHICLE_SELECT_DELAY': 6,  # Delay after "selecting" a vehicle
    'STATUS_DELAY': 4           # Delay after getting status
}

# MQTT Settings
MQTT_SETTINGS = {
    'DISCOVERY_PREFIX': 'homeassistant',  # Home Assistant MQTT discovery prefix
    'STATE_TOPIC': 'mazda/{vin}/state',   # Topic for vehicle state updates
    'COMMAND_TOPIC': 'mazda/{vin}/cmd',   # Topic for vehicle commands
    'AVAILABILITY_TOPIC': 'mazda/{vin}/available',  # Topic for availability status
    'QOS': 1,                            # MQTT QoS level (0, 1, or 2)
    'RETAIN': True,                      # Whether to retain messages
}

# Home Assistant entity configurations
HA_SENSORS = {
    'fuel_remaining': {
        'name': 'Fuel Remaining',
        'unit': '%',
        'icon': 'mdi:gas-station',
        'state_class': 'measurement'
    },
    'odometer': {
        'name': 'Odometer',
        'unit': 'mi',
        'icon': 'mdi:counter',
        'state_class': 'total_increasing'
    },
    'oil_life': {
        'name': 'Oil Life',
        'unit': '%',
        'icon': 'mdi:oil',
        'state_class': 'measurement'
    },
    'battery_voltage': {
        'name': 'Battery Voltage',
        'unit': 'V',
        'icon': 'mdi:car-battery',
        'state_class': 'measurement'
    }
}

HA_BINARY_SENSORS = {
    'doors_locked': {
        'name': 'Doors Locked',
        'device_class': 'lock',
        'payload_on': True,
        'payload_off': False
    },
    'hazard_lights': {
        'name': 'Hazard Lights',
        'device_class': 'safety',
        'payload_on': True,
        'payload_off': False
    },
    'parking_lights': {
        'name': 'Parking Lights',
        'device_class': 'light',
        'payload_on': True,
        'payload_off': False
    }
}

HA_BUTTONS = {
    'lock_doors': {
        'name': 'Lock Doors',
        'icon': 'mdi:car-door-lock',
        'command': 'door_lock'
    },
    'unlock_doors': {
        'name': 'Unlock Doors',
        'icon': 'mdi:car-door',
        'command': 'door_unlock'
    },
    'start_engine': {
        'name': 'Start Engine',
        'icon': 'mdi:engine',
        'command': 'engine_start'
    },
    'stop_engine': {
        'name': 'Stop Engine',
        'icon': 'mdi:engine-off',
        'command': 'engine_stop'
    },
    'hazard_lights': {
        'name': 'Toggle Hazard Lights',
        'icon': 'mdi:car-light-alert',
        'command': 'hazard_lights'
    }
}

# ==============================================
# Core Vehicle Data Functions
# ==============================================

async def get_vehicle_data(mazda: MazdaAPI, vehicle_id: str, data_type: str = 'status') -> Optional[Dict]:
    """Get vehicle data with enhanced error handling and retry logic"""
    max_retries = API_SETTINGS['MAX_RETRIES']
    base_delay = API_SETTINGS['RETRY_DELAY']
    
    for attempt in range(max_retries):
        try:
            if data_type == 'status':
                data = await mazda.get_vehicle_status(vehicle_id)
                if logger.getEffectiveLevel() == logging.DEBUG:
                    logger.debug(f"Vehicle {vehicle_id} status: {json.dumps(data, indent=2)}")
                return data
            elif data_type == 'health':
                data = await mazda.get_health_report(vehicle_id)
                if logger.getEffectiveLevel() == logging.DEBUG:
                    logger.debug(f"Vehicle {vehicle_id} health: {json.dumps(data, indent=2)}")
                return data
            else:
                raise ValueError(f"Invalid data type: {data_type}")
                
        except MazdaException as e:
            delay = base_delay * (attempt + 1)
            logger.error(f"Connection error retrieving {data_type} data: {str(e)}")
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed, waiting {delay}s before retry")
                await asyncio.sleep(delay)
                continue
            return None
            
        except MazdaException:
            # Login failed, try to refresh credentials
            logger.info("Login failed, refreshing credentials")
            try:
                await mazda.validate_credentials()
                continue
            except Exception as e:
                logger.error(f"Failed to refresh credentials: {e}")
                return None
                
        except MazdaException as e:
            # Other Mazda API errors
            logger.error(f"Mazda API error retrieving {data_type} data: {str(e)}")
            if attempt < max_retries - 1:
                delay = base_delay * (attempt + 1)
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed, waiting {delay}s before retry")
                await asyncio.sleep(delay)
                continue
            return None
            
        except Exception as e:
            # Unexpected errors
            logger.error(f"Unexpected error retrieving {data_type} data: {str(e)}")
            if attempt < max_retries - 1:
                delay = base_delay * (attempt + 1)
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed, waiting {delay}s before retry")
                await asyncio.sleep(delay)
                continue
            return None
    
    return None

async def process_vehicle_data(status: Dict, health: Dict, vehicle: Dict) -> Dict[str, Any]:
    """Process vehicle data with unified transformation"""
    try:
        # Basic validation
        if not isinstance(status, dict):
            raise ValueError("Invalid status data")
                
        # Process status data
        processed_data = {
            "id": vehicle.get("id"),
            "vin": vehicle.get("vin"),
            "nickname": vehicle.get("nickname", "Unknown"),
            
            # Status metrics
            "battery_level": status.get("batteryLevel"),
            "fuel_remaining": status.get("fuelRemainingPercent"),
            "fuel_distance": km_to_miles(status.get("fuelDistanceRemainingKm", 0)),
            "odometer": km_to_miles(status.get("odometerKm", 0)),
            
            # Door states
            "doors": {
                "driverDoorOpen": status.get("doorStatus", {}).get("driverDoorOpen", False),
                "passengerDoorOpen": status.get("doorStatus", {}).get("passengerDoorOpen", False),
                "rearLeftDoorOpen": status.get("doorStatus", {}).get("rearLeftDoorOpen", False),
                "rearRightDoorOpen": status.get("doorStatus", {}).get("rearRightDoorOpen", False),
                "trunkOpen": status.get("doorStatus", {}).get("trunkOpen", False),
                "hoodOpen": status.get("doorStatus", {}).get("hoodOpen", False),
                "fuelLidOpen": status.get("doorStatus", {}).get("fuelLidOpen", False)
            },
            
            # Window states
            "windows": {
                "driverWindowOpen": status.get("windowStatus", {}).get("driverWindowOpen", False),
                "passengerWindowOpen": status.get("windowStatus", {}).get("passengerWindowOpen", False),
                "rearLeftWindowOpen": status.get("windowStatus", {}).get("rearLeftWindowOpen", False),
                "rearRightWindowOpen": status.get("windowStatus", {}).get("rearRightWindowOpen", False)
            },
            
            # Door lock states
            "doorLocks": {
                "driverDoorUnlocked": status.get("doorLockStatus", {}).get("driverDoorUnlocked", False),
                "passengerDoorUnlocked": status.get("doorLockStatus", {}).get("passengerDoorUnlocked", False),
                "rearLeftDoorUnlocked": status.get("doorLockStatus", {}).get("rearLeftDoorUnlocked", False),
                "rearRightDoorUnlocked": status.get("doorLockStatus", {}).get("rearRightDoorUnlocked", False)
            },
            
            # Tire pressure
            "tirePressure": {
                "frontLeft": status.get("tirePressure", {}).get("frontLeft", 0),
                "frontRight": status.get("tirePressure", {}).get("frontRight", 0),
                "rearLeft": status.get("tirePressure", {}).get("rearLeft", 0),
                "rearRight": status.get("tirePressure", {}).get("rearRight", 0)
            }
        }
        
        # Add health report data if available
        if isinstance(health, dict):
            health_data = transform_health_report(health)
            if "maintenance" in health_data:
                processed_data["maintenance"] = health_data["maintenance"]
            if "tires" in health_data:
                # Merge tire pressure and warnings
                processed_data["tires"] = {
                    **processed_data.get("tirePressure", {}),
                    **health_data["tires"]
                }
            if "timestamp" in health_data:
                processed_data["health_timestamp"] = health_data["timestamp"]
        
        # Add timestamp
        processed_data["last_updated"] = datetime.now().isoformat()
        
        # Remove any None values
        return {k: v for k, v in processed_data.items() if v is not None}
            
    except Exception as e:
        logger.error(f"Error processing vehicle data: {e}")
        raise

async def update_vehicle(client: mqtt_client.Client, mazda: MazdaAPI, vehicle: Dict[str, Any], batch_mode: bool = False) -> None:
    """Update vehicle data and publish to MQTT"""
    try:
        vehicle_name = vehicle.get('nickname', vehicle['vin'])
        logger.info(f"Updating vehicle: {vehicle_name}")
        
        # Get vehicle status
        status = await get_vehicle_data(mazda, vehicle['id'], 'status')
        
        # Wait before getting health data to avoid rate limits
        await asyncio.sleep(UPDATE_INTERVALS['STATUS_HEALTH_DELAY'])
        
        # Get vehicle health
        health = await get_vehicle_data(mazda, vehicle['id'], 'health')
        
        # Process and publish data if we have either status or health
        if status or health:
            processed_data = await process_vehicle_data(status or {}, health or {}, vehicle)
            topic = MQTT_SETTINGS['STATE_TOPIC'].format(vin=vehicle['vin'])
            
            # Use publish method correctly without qos parameter
            if isinstance(client, MazdaMQTTClient):
                await client.publish(topic, json.dumps(processed_data), retain=True)
            else:
                client.publish(topic, json.dumps(processed_data))
                
            logger.info(f"Published update for vehicle {vehicle_name}")
        
        # Add delay between individual updates in sequential mode
        if not batch_mode:
            await asyncio.sleep(UPDATE_INTERVALS['INDIVIDUAL_UPDATE'])
            
    except Exception as e:
        logger.error(f"Failed to update vehicle {vehicle_name}: {e}")
        if not batch_mode:
            await asyncio.sleep(UPDATE_INTERVALS['ERROR_RETRY'])

# ==============================================
# MQTT Integration Functions
# ==============================================

class MazdaMQTTClient:
    def __init__(self, host: str, port: int, username: str = None, password: str = None):
        """Initialize MQTT client with proper callbacks"""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None
        self.mazda_client = None
        self.vehicle_controller = None
        self._connected = False
        self._connection_lock = asyncio.Lock()
        self._reconnect_delay = 1
        self._max_reconnect_delay = 60
        
        # Initialize components
        self._setup_client()
    
    def _setup_client(self):
        """Set up MQTT client with proper protocol and callbacks"""
        self.client = mqtt.Client(protocol=mqtt.MQTTv5)
        
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
            
        # Set up callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        
        # Set up last will message
        self.client.will_set(
            MQTT_SETTINGS['STATE_TOPIC'].format(vin=""),
            payload="offline",
            qos=1,
            retain=True
        )
        
    async def connect(self, timeout: int = 10) -> bool:
        """Connect to MQTT broker with timeout and proper error handling"""
        async with self._connection_lock:
            if self._connected:
                return True
                
            try:
                self.client.connect(self.host, self.port)
                self.client.loop_start()
                
                start_time = time.time()
                while not self._connected and (time.time() - start_time) < timeout:
                    await asyncio.sleep(0.1)
                    
                if not self._connected:
                    logger.error("Failed to connect to MQTT broker within timeout")
                    self.client.loop_stop()
                    return False
                    
                # Publish online status
                self.client.publish(
                    MQTT_SETTINGS['STATE_TOPIC'].format(vin=""),
                    payload="online",
                    qos=1,
                    retain=True
                )
                return True
                
            except Exception as e:
                logger.error(f"Error connecting to MQTT broker: {e}")
                return False
                
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Handle MQTT connection with improved error handling"""
        if reason_code.is_failure:
            logger.error(f"Failed to connect to MQTT broker: {reason_code}")
            self._connected = False
            return
            
        logger.info("Connected to MQTT broker")
        self._connected = True
        self._reconnect_delay = 1  # Reset backoff delay
        
        # Subscribe to command topics
        client.subscribe(f"{MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]}/+/cmd")
        
    def _on_disconnect(self, client, userdata, reason_code, properties):
        """Handle MQTT disconnection with backoff retry"""
        logger.warning(f"Disconnected from MQTT broker: {reason_code}")
        self._connected = False
        
        if reason_code != 0:
            delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
            logger.info(f"Will attempt reconnection in {delay} seconds")
            self._reconnect_delay = delay

    def _on_message(self, client, userdata, message):
        """Handle incoming MQTT messages"""
        try:
            topic = message.topic
            payload = message.payload.decode()
            logger.debug(f"Received message on topic {topic}: {payload}")
            
            # Handle command messages
            if topic.startswith(MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]) and topic.endswith('/cmd'):
                vin = topic.split('/')[1]  # Extract VIN from topic
                asyncio.create_task(self.handle_command(vin, payload, self.mazda_client))
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    def _on_publish(self, client, userdata, mid):
        """Handle successful message publication"""
        logger.debug(f"Message {mid} published successfully")

    async def process_vehicle_data(self, status: Dict, health: Dict, vehicle: Dict) -> Dict[str, Any]:
        """Process vehicle data with unified transformation
        Modified: 2025-01-16 12:51:16 -06:00
        - Fixed vehicle ID usage
        - Improved data validation
        """
        try:
            # Basic validation
            if not isinstance(status, dict) or not isinstance(health, dict):
                raise ValueError("Invalid status or health data")
                
            # Process status data
            processed_data = {
                "id": vehicle.get("id"),  # Internal ID
                "vin": vehicle.get("vin"),  # External VIN
                "nickname": vehicle.get("nickname", "Unknown"),
                
                # Status metrics
                "battery_level": status.get("batteryLevel"),
                "fuel_remaining": status.get("fuelRemainingPercent"),
                "fuel_distance_miles": km_to_miles(status.get("fuelDistanceRemainingKm")),  # Use direct mile value
                "odometer_miles": km_to_miles(status.get("odometerKm")),  # Use direct mile value
                "doors_locked": status.get("doorLocked", False),
                
                # Health metrics
                "health": transform_health_report(health),
                
                # Timestamps
                "last_updated": datetime.now().isoformat()
            }
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Failed to process vehicle data: {e}")
            raise
    
    async def publish_vehicle_data(self, data: Dict[str, Any], vin: str):
        """Publish vehicle data to MQTT
        Modified: 2025-01-16 13:06:00 -06:00
        - Fixed topic structure
        - Added data validation
        - Improved error handling
        """
        try:
            if not data or not vin:
                raise ValueError("Missing data or VIN")
            
            # Add timestamp if not present
            if 'last_updated' not in data:
                data['last_updated'] = datetime.now().isoformat()
            
            # Publish main state
            topic = MQTT_SETTINGS['STATE_TOPIC'].format(vin=vin)
            success = await self.publish(topic, json.dumps(data), retain=True)
            
            if not success:
                logger.error(f"Failed to publish state for vehicle {vin}")
                return
            
            # Publish individual sensors for Home Assistant
            base_topic = f"{MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]}/{vin}"
            
            # Status sensors
            if 'fuel_remaining' in data:
                await self.publish(f"{base_topic}/fuel_remaining", str(data['fuel_remaining']), retain=True)
            if 'odometer_miles' in data:
                await self.publish(f"{base_topic}/odometer", str(data['odometer_miles']), retain=True)
            
            # Health sensors
            if 'health' in data:
                health = data['health']
                if 'tires' in health:
                    for position, pressure in health['tires']['pressures'].items():
                        if pressure is not None:
                            await self.publish(f"{base_topic}/tire_pressure/{position}", str(pressure), retain=True)
                if 'maintenance' in health:
                    if 'oil' in health['maintenance']:
                        await self.publish(f"{base_topic}/oil_life", str(health['maintenance']['oil']['remainingPercentage']), retain=True)
                if 'warnings' in health:
                    for warning, state in health['warnings'].items():
                        await self.publish(f"{base_topic}/warning/{warning}", str(state).lower(), retain=True)
            
            logger.debug(f"Successfully published all data for vehicle {vin}")
            
        except Exception as e:
            logger.error(f"Error publishing vehicle data: {e}")
            raise

    async def publish_command_result(self, vin: str, command: str, success: bool, message: str = None):
        """Publish command execution result"""
        try:
            topic = MQTT_SETTINGS['STATE_TOPIC'].format(vin=vin)
            payload = {
                "command": command,
                "success": success,
                "timestamp": datetime.now().isoformat()
            }
            if message:
                payload["message"] = message
                
            await self.publish(topic, json.dumps(payload))
            
        except Exception as e:
            logger.error(f"Failed to publish command result: {e}")
    
    def set_mazda_client(self, mazda_client: MazdaAPI):
        """Set Mazda client and initialize vehicle controller"""
        self.mazda_client = mazda_client
        self.vehicle_controller = VehicleController(mazda_client, self)
    
    async def publish(self, topic: str, payload: str, retain: bool = False) -> bool:
        """Publish message to MQTT with validation"""
        try:
            if not self._connected:
                raise RuntimeError("Not connected to MQTT broker")
                
            result = self.client.publish(topic, payload, qos=1, retain=retain)
            return result.rc == mqtt_client.MQTT_ERR_SUCCESS
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    async def setup_ha_discovery(self, vin: str, vehicle_info: Dict[str, Any]):
        """Set up Home Assistant MQTT discovery for a vehicle"""
        try:
            device_id = f"mazda-{vin}"
            device_info = {
                "identifiers": [device_id],
                "manufacturer": "MAZDA",
                "model": vehicle_info["modelName"],
                "name": vehicle_info.get("nickname", vin),
                "serial_number": vin
            }

            # Set up sensors
            for sensor_id, config in HA_SENSORS.items():
                discovery_topic = f"{MQTT_SETTINGS['DISCOVERY_PREFIX']}/sensor/{device_id}/{sensor_id}/config"
                sensor_config = {
                    "name": f"{device_info['name']} {config['name']}",
                    "unique_id": f"{device_id}_{sensor_id}",
                    "state_topic": MQTT_SETTINGS['STATE_TOPIC'].format(vin=vin),
                    "unit_of_measurement": config.get('unit'),
                    "icon": config.get('icon'),
                    "state_class": config.get('state_class'),
                    "device_class": config.get('device_class'),
                    "value_template": "{{ value_json." + config.get('value_template', '') + " }}",
                    "device": device_info
                }
                await self.publish(discovery_topic, json.dumps(sensor_config), retain=True)

            # Set up binary sensors
            for sensor_id, config in HA_BINARY_SENSORS.items():
                discovery_topic = f"{MQTT_SETTINGS['DISCOVERY_PREFIX']}/binary_sensor/{device_id}/{sensor_id}/config"
                sensor_config = {
                    "name": f"{device_info['name']} {config['name']}",
                    "unique_id": f"{device_id}_{sensor_id}",
                    "state_topic": MQTT_SETTINGS['STATE_TOPIC'].format(vin=vin),
                    "device_class": config.get('device_class'),
                    "payload_on": config.get('payload_on', True),
                    "payload_off": config.get('payload_off', False),
                    "value_template": "{{ value_json." + config.get('value_template', '') + " }}",
                    "device": device_info
                }
                await self.publish(discovery_topic, json.dumps(sensor_config), retain=True)

            # Set up buttons
            for button_id, config in HA_BUTTONS.items():
                discovery_topic = f"{MQTT_SETTINGS['DISCOVERY_PREFIX']}/button/{device_id}/{button_id}/config"
                command_topic = MQTT_SETTINGS['COMMAND_TOPIC'].format(vin=vin)
                button_config = {
                    "name": f"{device_info['name']} {config['name']}",
                    "unique_id": f"{device_id}_{button_id}",
                    "command_topic": command_topic,
                    "payload_press": config.get('payload', button_id),
                    "device": device_info
                }
                await self.publish(discovery_topic, json.dumps(button_config), retain=True)

            logger.info(f"Successfully set up Home Assistant discovery for vehicle {device_info['name']}")

        except Exception as e:
            logger.error(f"Error setting up Home Assistant discovery for vehicle {vin}: {e}")
            raise

    async def publish_vehicle_data(self, data: Dict[str, Any], vin: str):
        """Publish vehicle data to MQTT with Home Assistant support
        Modified: 2025-01-16 13:07:09 -06:00
        - Added individual sensor states
        - Added binary sensor states
        - Improved error handling
        """
        try:
            if not data or not vin:
                raise ValueError("Missing data or VIN")

            # Add timestamp if not present
            if 'last_updated' not in data:
                data['last_updated'] = datetime.now().isoformat()

            # Publish main state
            topic = MQTT_SETTINGS['STATE_TOPIC'].format(vin=vin)
            success = await self.publish(topic, json.dumps(data), retain=True)

            if not success:
                logger.error(f"Failed to publish state for vehicle {vin}")
                return

            # Publish sensor states
            for sensor_id in HA_SENSORS:
                if sensor_id in data:
                    await self.publish(
                        f"{MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]}/{vin}/{sensor_id}",
                        str(data[sensor_id]),
                        retain=True
                    )

            # Publish binary sensor states
            if 'doors' in data:
                await self.publish(
                    f"{MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]}/{vin}/doors_locked",
                    str(data['doors'].get('locked', False)).lower(),
                    retain=True
                )

            if 'lights' in data:
                lights = data['lights']
                await self.publish(
                    f"{MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]}/{vin}/hazard_lights",
                    str(lights.get('hazard_lights', False)).lower(),
                    retain=True
                )
                await self.publish(
                    f"{MQTT_SETTINGS['STATE_TOPIC'].split('/')[0]}/{vin}/parking_lights",
                    str(lights.get('parking_lights', False)).lower(),
                    retain=True
                )

            logger.debug(f"Successfully published all data for vehicle {vin}")

        except Exception as e:
            logger.error(f"Error publishing vehicle data: {e}")
            raise

    async def handle_command(self, vin: str, command: str, mazda: MazdaAPI):
        """Handle commands from Home Assistant
        Modified: 2025-01-16 13:07:09 -06:00
        - Added command handling
        - Added response publishing
        """
        try:
            success = False
            message = None

            if command == 'door_lock':
                success = await mazda.lock_doors(vin)
                message = "Doors locked successfully" if success else "Failed to lock doors"
            elif command == 'door_unlock':
                success = await mazda.unlock_doors(vin)
                message = "Doors unlocked successfully" if success else "Failed to unlock doors"
            elif command == 'engine_start':
                success = await mazda.start_engine(vin)
                message = "Engine started successfully" if success else "Failed to start engine"
            elif command == 'engine_stop':
                success = await mazda.stop_engine(vin)
                message = "Engine stopped successfully" if success else "Failed to stop engine"
            elif command == 'hazard_lights':
                success = await mazda.toggle_hazard_lights(vin)
                message = "Hazard lights toggled successfully" if success else "Failed to toggle hazard lights"
            else:
                message = f"Unknown command: {command}"

            # Publish command result
            await self.publish_command_result(vin, command, success, message)

        except Exception as e:
            logger.error(f"Error handling command {command} for vehicle {vin}: {e}")
            await self.publish_command_result(vin, command, False, str(e))

    def cleanup(self):
        """Clean up resources"""
        if self.client:
            try:
                self.client.publish(MQTT_SETTINGS['STATE_TOPIC'].format(vin=""), "offline", qos=1, retain=True)
                self.client.loop_stop()
                self.client.disconnect()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

# ==============================================
# Vehicle Control Functions
# ==============================================

class VehicleController:
    """Controller for vehicle commands with proper error handling"""
    
    def __init__(self, mazda: MazdaAPI, mqtt: MazdaMQTTClient):
        self.mazda = mazda
        self.mqtt = mqtt
        self._command_lock = asyncio.Lock()
        self._vehicle_cache = {}  # Cache vehicle info
        
        # Log available commands
        self._available_commands = {
            'lock': 'Lock vehicle doors',
            'unlock': 'Unlock vehicle doors',
            'start_engine': 'Start vehicle engine',
            'stop_engine': 'Stop vehicle engine',
            'hazard_lights': 'Toggle hazard lights'
        }
        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug("Available vehicle commands:")
            for cmd, desc in self._available_commands.items():
                logger.debug(f"{cmd}: {desc}")
    
    async def get_vehicle_info(self, vin: str) -> Optional[Dict]:
        """Get vehicle info from cache or API"""
        if vin not in self._vehicle_cache:
            vehicles = await get_basic_vehicle_info(self.mazda)
            self._vehicle_cache = {v['vin']: v for v in vehicles}
        return self._vehicle_cache.get(vin)
    
    async def execute_command(self, vin: str, command: str, params: Dict = None) -> Dict:
        """Execute vehicle command with proper error handling"""
        async with self._command_lock:
            try:
                # Get vehicle info
                vehicle = await self.get_vehicle_info(vin)
                if not vehicle:
                    raise ValueError(f"Vehicle not found: {vin}")
                
                # Use internal ID for API calls
                vehicle_id = vehicle['id']
                
                # Execute command
                if command == 'lock':
                    await self.mazda.lock_doors(vehicle_id)
                elif command == 'unlock':
                    await self.mazda.unlock_doors(vehicle_id)
                elif command == 'start_engine':
                    await self.mazda.start_engine(vehicle_id)
                elif command == 'stop_engine':
                    await self.mazda.stop_engine(vehicle_id)
                elif command == 'hazard_lights':
                    await self.mazda.turn_on_hazard_lights(vehicle_id)
                else:
                    raise ValueError(f"Unknown command: {command}")
                
                # Publish success
                await self.mqtt.publish_command_result(vin, command, True)
                
                return {'success': True}
                
            except Exception as e:
                error_msg = f"Failed to execute command {command} for vehicle {vin}: {e}"
                logger.error(error_msg)
                
                # Publish failure
                await self.mqtt.publish_command_result(vin, command, False, str(e))
                
                raise

# ==============================================
# Home Assistant Device Configuration
# ==============================================

# Define all sensors with proper units and templates
sensors = [
    {
        "name": "odometer",
        "unit": "mi",
        "dev_cla": "distance",
        "ic": "mdi:counter",
        "tpl": "odometer"
    },
    {
        "name": "fuel_remaining",
        "unit": "%",
        "dev_cla": "battery",  # Using battery class for percentage
        "ic": "mdi:gas-station",
        "tpl": "fuelRemainingPercent"
    },
    {
        "name": "fuel_distance",
        "unit": "mi",
        "dev_cla": "distance",
        "ic": "mdi:map-marker-distance",
        "tpl": "fuelDistanceRemainingMile"
    },
    {
        "name": "latitude",
        "dev_cla": "latitude",
        "ic": "mdi:latitude",
        "tpl": "latitude"
    },
    {
        "name": "longitude",
        "dev_cla": "longitude",
        "ic": "mdi:longitude",
        "tpl": "longitude"
    },
    {
        "name": "last_update",
        "dev_cla": "timestamp",
        "tpl": "lastUpdatedTimestamp"
    },
    {
        "name": "position_timestamp",
        "dev_cla": "timestamp",
        "tpl": "positionTimestamp"
    },
    {
        "name": "front_left_tire_pressure",
        "unit": "PSI",
        "dev_cla": "pressure",
        "ic": "mdi:tire",
        "tpl": "tirePressure.frontLeftTirePressurePsi"
    },
    {
        "name": "front_right_tire_pressure",
        "unit": "PSI",
        "dev_cla": "pressure",
        "ic": "mdi:tire",
        "tpl": "tirePressure.frontRightTirePressurePsi"
    },
    {
        "name": "rear_left_tire_pressure",
        "unit": "PSI",
        "dev_cla": "pressure",
        "ic": "mdi:tire",
        "tpl": "tirePressure.rearLeftTirePressurePsi"
    },
    {
        "name": "rear_right_tire_pressure",
        "unit": "PSI",
        "dev_cla": "pressure",
        "ic": "mdi:tire",
        "tpl": "tirePressure.rearRightTirePressurePsi"
    },
    {
        "name": "oil_maintenance_distance",
        "unit": "mi",
        "dev_cla": "distance",
        "ic": "mdi:oil",
        "tpl": "maintenance.oilMaintenance.distanceRemaining"
    },
    {
        "name": "oil_life",
        "unit": "%",
        "dev_cla": "battery",  # Using battery class for percentage
        "ic": "mdi:oil",
        "tpl": "maintenance.oilMaintenance.remainingPercentage"
    },
    {
        "name": "maintenance_distance",
        "unit": "mi",
        "dev_cla": "distance",
        "ic": "mdi:wrench",
        "tpl": "maintenance.regularMaintenance.distanceRemaining"
    },
    {
        "name": "maintenance_interval",
        "unit": "mi",
        "dev_cla": "distance",
        "ic": "mdi:wrench-clock",
        "tpl": "maintenance.regularMaintenance.setDistance"
    }
]

# Define binary sensors
binary_sensors = [
    {
        "name": "driver_door",
        "dev_cla": "door",
        "tpl": "doors.driverDoorOpen"
    },
    {
        "name": "passenger_door",
        "dev_cla": "door",
        "tpl": "doors.passengerDoorOpen"
    },
    {
        "name": "rear_left_door",
        "dev_cla": "door",
        "tpl": "doors.rearLeftDoorOpen"
    },
    {
        "name": "rear_right_door",
        "dev_cla": "door",
        "tpl": "doors.rearRightDoorOpen"
    },
    {
        "name": "trunk",
        "dev_cla": "door",
        "tpl": "doors.trunkOpen"
    },
    {
        "name": "hood",
        "dev_cla": "door",
        "tpl": "doors.hoodOpen"
    },
    {
        "name": "fuel_door",
        "dev_cla": "door",
        "tpl": "doors.fuelLidOpen"
    },
    {
        "name": "driver_door_lock",
        "dev_cla": "lock",
        "tpl": "doorLocks.driverDoorUnlocked"
    },
    {
        "name": "passenger_door_lock",
        "dev_cla": "lock",
        "tpl": "doorLocks.passengerDoorUnlocked"
    },
    {
        "name": "rear_left_door_lock",
        "dev_cla": "lock",
        "tpl": "doorLocks.rearLeftDoorUnlocked"
    },
    {
        "name": "rear_right_door_lock",
        "dev_cla": "lock",
        "tpl": "doorLocks.rearRightDoorUnlocked"
    },
    {
        "name": "driver_window",
        "dev_cla": "window",
        "tpl": "windows.driverWindowOpen"
    },
    {
        "name": "passenger_window",
        "dev_cla": "window",
        "tpl": "windows.passengerWindowOpen"
    },
    {
        "name": "rear_left_window",
        "dev_cla": "window",
        "tpl": "windows.rearLeftWindowOpen"
    },
    {
        "name": "rear_right_window",
        "dev_cla": "window",
        "tpl": "windows.rearRightWindowOpen"
    },
    {
        "name": "hazard_lights",
        "dev_cla": "light",
        "tpl": "hazardLightsOn"
    },
    {
        "name": "oil_low_warning",
        "dev_cla": "problem",
        "tpl": "warnings.oilLow"
    },
    {
        "name": "tire_pressure_warning",
        "dev_cla": "problem",
        "tpl": "warnings.tirePressureLow"
    },
    {
        "name": "tpms_system_fault",
        "dev_cla": "problem",
        "tpl": "warnings.tpmsSystemFault"
    },
    {
        "name": "oil_amount_exceed",
        "dev_cla": "problem",
        "tpl": "warnings.oilAmountExceed"
    }
]

# ==============================================
# Data Transformation Functions
# ==============================================

def km_to_miles(km):
    """Convert kilometers to miles with proper rounding"""
    if km is None:
        return 0
    return round(km * 0.621371, 1)

def transform_vehicle_status(vehicle_status):
    if not vehicle_status:
        return {}

    def km_to_miles(km):
        if km is not None:
            return round(km * 0.621371, 1)
        return 0

    # Get odometer from health report if available, otherwise convert from km
    odometer_miles = vehicle_status.get("healthReport", {}).get("OdoDispValueMile")
    if not odometer_miles:
        odometer_miles = km_to_miles(vehicle_status.get("odometerKm"))

    # Convert fuel distance to miles if available
    fuel_distance = km_to_miles(vehicle_status.get("fuelDistanceRemainingKm")) if vehicle_status.get("fuelDistanceRemainingKm") else 0
    
    # Build response with only what we need, no nulls
    response = {
        "vin": vehicle_status.get("vin"),
        "nickname": vehicle_status.get("nickname"),
        "fuel": {
            "level": vehicle_status.get("fuelRemainingPercent", 0),
            "range": fuel_distance  # In miles
        },
        "odometer": odometer_miles,  # In miles
        "doors": {
            "driver": vehicle_status.get("doors", {}).get("driverDoorOpen", False),
            "passenger": vehicle_status.get("doors", {}).get("passengerDoorOpen", False),
            "rear_left": vehicle_status.get("doors", {}).get("rearLeftDoorOpen", False),
            "rear_right": vehicle_status.get("doors", {}).get("rearRightDoorOpen", False),
            "trunk": vehicle_status.get("doors", {}).get("trunkOpen", False),
            "hood": vehicle_status.get("doors", {}).get("hoodOpen", False),
            "fuel_lid": vehicle_status.get("doors", {}).get("fuelLidOpen", False)
        },
        "locks": {
            "driver": not vehicle_status.get("doorLocks", {}).get("driverDoorUnlocked", False),
            "passenger": not vehicle_status.get("doorLocks", {}).get("passengerDoorUnlocked", False),
            "rear_left": not vehicle_status.get("doorLocks", {}).get("rearLeftDoorUnlocked", False),
            "rear_right": not vehicle_status.get("doorLocks", {}).get("rearRightDoorUnlocked", False)
        }
    }

    # Only include tire pressures if they exist
    tire_pressure = vehicle_status.get("tirePressure", {})
    if any(tire_pressure.values()):
        response["tires"] = {
            "front_left": tire_pressure.get("frontLeftTirePressurePsi"),
            "front_right": tire_pressure.get("frontRightTirePressurePsi"),
            "rear_left": tire_pressure.get("rearLeftTirePressurePsi"),
            "rear_right": tire_pressure.get("rearRightTirePressurePsi")
        }

    return response

def transform_health_report(health_report):
    """Transform health report data into a consistent format"""
    if not health_report or 'remoteInfos' not in health_report:
        return {}
    
    try:
        info = health_report['remoteInfos'][0]
        
        # Extract tire pressure warnings
        tpms_info = info.get('TPMSInformation', {})
        tires = {
            "warnings": {
                "front_left": bool(tpms_info.get('FLTyrePressWarn', 0)),
                "front_right": bool(tpms_info.get('FRTyrePressWarn', 0)),
                "rear_left": bool(tpms_info.get('RLTyrePressWarn', 0)),
                "rear_right": bool(tpms_info.get('RRTyrePressWarn', 0))
            },
            "system_status": not bool(info.get('WngTpmsStatus', 0))
        }
        
        # Extract maintenance info
        maintenance = {}
        oil_info = info.get('OilMntInformation', {})
        if 'RemOilDistMile' in oil_info:
            maintenance["oil"] = {
                "remaining_miles": oil_info['RemOilDistMile'],
                "warning": bool(info.get('WngOilShortage', 0))
            }
            
        reg_info = info.get('RegularMntInformation', {})
        if reg_info.get('RemRegDistMile') != 40960:  # Filter out placeholder value
            maintenance["regular"] = {
                "remaining_miles": reg_info.get('RemRegDistMile'),
                "interval_miles": reg_info.get('MntSetDistMile')
            }
            
        return {
            "tires": tires,
            "maintenance": maintenance,
            "timestamp": info.get('OccurrenceDate')
        }
        
    except Exception as e:
        logger.error(f"Error transforming health report: {e}")
        return {}

# ==============================================
# Main Processing Functions
# ==============================================

async def get_basic_vehicle_info(mazda: MazdaAPI):
    """Get basic vehicle information (lightweight call)
    Modified: 2025-01-16 11:36:28 -06:00
    Added retry logic and improved error handling
    """
    max_retries = API_SETTINGS['MAX_RETRIES']
    retry_delay = API_SETTINGS['RETRY_DELAY']
    last_exception = None

    for attempt in range(max_retries):
        try:
            vehicles = await mazda.get_vehicles()
            logger.info(f"Successfully retrieved {len(vehicles)} vehicles")
            return vehicles

        except aiohttp.ClientError as e:
            last_exception = e
            logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Full exception details: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                
                # Recreate client on connection errors
                if isinstance(e, (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError)):
                    logger.info("Recreating Mazda client due to connection error")
                    mazda = await create_mazda_client()
            
    logger.error(f"Failed to get basic vehicle info after {max_retries} attempts")
    raise last_exception

async def create_mazda_client():
    """Create a new Mazda client instance
    Created: 2025-01-16 11:36:28 -06:00
    """
    email = os.getenv("MAZDA_EMAIL")
    password = os.getenv("MAZDA_PASSWORD")
    region = os.getenv("MAZDA_REGION", "MNAO")

    if not all([email, password]):
        raise ValueError("Missing required Mazda credentials in environment variables")
        
    logger.info(f"Initializing Mazda client for region: {region}")
    return MazdaAPI(email, password, region)

async def process_vehicles(client: MazdaMQTTClient, mazda: MazdaAPI, vehicles: List[Dict[str, Any]]) -> None:
    """Process vehicle updates with configurable batch or sequential mode"""
    try:
        # Set up Home Assistant discovery for all vehicles first
        logger.info("Setting up Home Assistant discovery for all vehicles")
        for vehicle in vehicles:
            await client.setup_ha_discovery(vehicle['vin'], vehicle)
            logger.info(f"Set up Home Assistant discovery for vehicle: {vehicle.get('nickname', vehicle['vin'])}")
        
        while True:  # Main update loop
            try:
                # Phase 1: Status Updates
                if UPDATE_INTERVALS['ENABLE_STATUS_UPDATES']:
                    logger.info("Starting status update phase for all vehicles")
                    for vehicle in vehicles:
                        try:
                            vehicle_name = vehicle.get('nickname', vehicle['vin'])
                            logger.info(f"Getting status for vehicle: {vehicle_name}")
                            
                            # Get and publish status
                            status = await get_vehicle_data(mazda, vehicle['id'], 'status')
                            if status:
                                processed_data = await process_vehicle_data(status, {}, vehicle)
                                await client.publish_vehicle_data(processed_data, vehicle['vin'])
                                logger.info(f"Published status update for vehicle {vehicle_name}")
                            
                            # Wait before next vehicle
                            await asyncio.sleep(UPDATE_INTERVALS['STATUS_DELAY'])
                            
                        except Exception as e:
                            logger.error(f"Error updating status for {vehicle_name}: {e}")
                            continue
                
                # Wait between phases
                logger.info(f"Waiting {UPDATE_INTERVALS['PHASE_SWITCH_DELAY']}s before health updates")
                await asyncio.sleep(UPDATE_INTERVALS['PHASE_SWITCH_DELAY'])
                
                # Phase 2: Health Updates
                if UPDATE_INTERVALS['ENABLE_HEALTH_UPDATES']:
                    logger.info("Starting health update phase for all vehicles")
                    for vehicle in vehicles:
                        try:
                            vehicle_name = vehicle.get('nickname', vehicle['vin'])
                            logger.info(f"Getting health for vehicle: {vehicle_name}")
                            
                            # Get and publish health
                            health = await get_vehicle_data(mazda, vehicle['id'], 'health')
                            if health:
                                processed_data = await process_vehicle_data({}, health, vehicle)
                                await client.publish_vehicle_data(processed_data, vehicle['vin'])
                                logger.info(f"Published health update for vehicle {vehicle_name}")
                            
                            # Wait before next vehicle
                            await asyncio.sleep(UPDATE_INTERVALS['HEALTH_DELAY'])
                            
                        except Exception as e:
                            logger.error(f"Error updating health for {vehicle_name}: {e}")
                            continue
                
                # Wait before next complete cycle
                logger.info(f"Completed update cycle. Waiting {UPDATE_INTERVALS['BATCH_UPDATE']}s before next cycle")
                await asyncio.sleep(UPDATE_INTERVALS['BATCH_UPDATE'])
                
            except Exception as e:
                logger.error(f"Error in update cycle: {e}")
                await asyncio.sleep(UPDATE_INTERVALS['ERROR_RETRY'])
                
    except Exception as e:
        logger.error(f"Critical error in process_vehicles: {e}")
        raise

# ==============================================
# Program Entry Point
# ==============================================

async def main():
    """Main program entry point
    Modified: 2025-01-16 13:11:24 -06:00
    - Fixed pymazda import and client creation
    - Added proper cleanup
    """
    client = None
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Get MQTT configuration
        mqtt_broker = os.getenv('MQTT_BROKER', 'localhost')
        mqtt_port = int(os.getenv('MQTT_PORT', '1883'))
        mqtt_username = os.getenv('MQTT_USERNAME')
        mqtt_password = os.getenv('MQTT_PASSWORD')
        
        # Get Mazda credentials
        mazda_email = os.getenv('MAZDA_EMAIL')
        mazda_password = os.getenv('MAZDA_PASSWORD')
        mazda_region = os.getenv('MAZDA_REGION', 'NA')
        
        if not all([mazda_email, mazda_password]):
            raise ValueError("Missing Mazda credentials")
        
        # Initialize MQTT client
        client = MazdaMQTTClient(mqtt_broker, mqtt_port, mqtt_username, mqtt_password)
        
        # Connect to MQTT broker
        if not await client.connect():
            raise RuntimeError("Failed to connect to MQTT broker")
            
        logger.info("Successfully connected to MQTT broker")
        
        # Initialize Mazda client
        mazda = MazdaAPI(mazda_email, mazda_password, mazda_region)
        await mazda.validate_credentials()
        client.mazda_client = mazda  # Store mazda client for command handling
        
        # Get vehicle information
        vehicles = await get_basic_vehicle_info(mazda)
        logger.info(f"Successfully retrieved {len(vehicles)} vehicles")
        
        # Start processing vehicles
        await process_vehicles(client, mazda, vehicles)
        
    except Exception as e:
        logger.error(f"Critical error in main: {str(e)}")
        if client:
            client.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())