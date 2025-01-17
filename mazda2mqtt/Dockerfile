FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install required system packages and Python
RUN apt-get update && \
    apt-get install -y wget unzip python3 python3-venv && \
    python3 -m venv venv && \
    ./venv/bin/python3 -m pip install aiohttp paho-mqtt cryptography python-dotenv

# Download the Mazda integration from the external GitHub repo
RUN wget https://github.com/snake2715/home-assistant-mazda/archive/refs/heads/main.zip && \
    unzip main.zip && rm -rf main.zip

# Create necessary directories
RUN mkdir -p /app

# Copy the local files into the container
COPY . /app/

# Set permissions
RUN chmod +x /app/run.sh

# Use the run.sh script as the entry point
CMD ["/bin/bash", "/app/run.sh"]