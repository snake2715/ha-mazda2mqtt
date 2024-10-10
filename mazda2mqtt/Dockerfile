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

# Copy the local app.py and run.sh into the container
COPY app.py /app/home-assistant-mazda-main/custom_components/mazda/app.py
COPY run.sh /app/run.sh

# Set permissions and set the entry point
RUN chmod +x /app/run.sh

# Use the run.sh script as the entry point
CMD ["/app/run.sh"]
