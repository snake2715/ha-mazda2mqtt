#!/usr/bin/with-contenv bashio

# Get configuration options from Home Assistant add-on config
MAZDA_EMAIL=$(bashio::config 'mazda_email')    # Changed from mazda_username
MAZDA_PASSWORD=$(bashio::config 'mazda_password')
MAZDA_REGION=$(bashio::config 'mazda_region')
MQTT_BROKER=$(bashio::config 'mqtt_host')
MQTT_PORT=$(bashio::config 'mqtt_port')
MQTT_USER=$(bashio::config 'mqtt_user')
MQTT_PASSWORD=$(bashio::config 'mqtt_password')

# Export environment variables for use in Python app
export MAZDA_EMAIL     # Changed from MAZDA_USERNAME
export MAZDA_PASSWORD
export MAZDA_REGION
export MQTT_BROKER
export MQTT_PORT
export MQTT_USER
export MQTT_PASSWORD

# Debug print to verify environment variables (optional, remove for production)
echo "Mazda Email: $MAZDA_EMAIL"    # Changed from Username
echo "Mazda Region: $MAZDA_REGION"

# Run the Python application
exec ./venv/bin/python3 /app/home-assistant-mazda-main/custom_components/mazda/app.py