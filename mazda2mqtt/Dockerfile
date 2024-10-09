FROM debian:latest

# Set working directory inside the container
WORKDIR /app

# Install required system packages and Python
RUN apt-get update && \
    apt-get install -y wget unzip python3 python3-venv && \
    python3 -m venv venv && \
    ./venv/bin/python3 -m pip install aiohttp paho-mqtt cryptography python-dotenv

# Download the Mazda integration from the external GitHub repo
RUN wget https://github.com/runDMCA/home-assistant-mazda/archive/refs/heads/main.zip && \
    unzip main.zip && rm -rf main.zip

# Copy the local app.py into the correct directory
COPY app.py /app/home-assistant-mazda-main/custom_components/mazda/app.py

# Set shell to ensure the correct environment for running scripts
SHELL ["/bin/ash", "-c"]

# Set environment variables from the Home Assistant configuration (these will be set dynamically)
ENV MQTT_BROKER core-mosquitto
ENV MQTT_PORT 1883
ENV MQTT_USER ""
ENV MQTT_PASSWORD ""
ENV MAZDA_USERNAME ""
ENV MAZDA_PASSWORD ""
ENV MAZDA_REGION "MNAO"

# Command to run the Mazda app
CMD ["./venv/bin/python3", "./home-assistant-mazda-main/custom_components/mazda/app.py"]
