# Mazda2MQTT Home Assistant Add-on

This is a Home Assistant add-on for the [Mazda2MQTT](https://github.com/brunob45/mazda2mqtt) bridge code, created by [brunob45](https://github.com/brunob45). It allows for integrating your Mazda vehicle data into Home Assistant with easy configuration for MQTT and region settings.

## Installation Instructions

1. Open your Home Assistant instance.
2. Navigate to **Settings** > **Add-ons** > **Add-on Store**.
3. Click on the three dots in the upper right corner and select **Repositories**.
4. Add the following repository URL:
https://github.com/scruysberghs/ha-mazda2mqtt

5. Once added, the Mazda2MQTT add-on will appear in the list of available add-ons.

You can use the button below to add this repository to your Home Assistant instance automatically:

[![Add Repository to Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https://github.com/scruysberghs/ha-mazda2mqtt)

## Configuration Options

### Mazda Account
- **mazda_username**: Your Mazda account username (required).
- **mazda_password**: Your Mazda account password (required).

### Region Selection
- **mazda_region**: Select your region:
- `MNAO` for America
- `MME` for Europe
- `MJO` for Japan

### MQTT Configuration
By default, the add-on is configured to use Home Assistant's built-in MQTT broker. If you'd like to use another broker, you can provide your own MQTT host, username, and password in the configuration options.

## Credits

- **Mazda2MQTT Bridge**: Thanks to [brunob45](https://github.com/brunob45) for creating the Mazda2MQTT bridge code, which makes this integration possible.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Known issues
- Some users are not able to connect to the Mazda API using the debian base image.
Try to swap the base image in the Dockerfile.

Change the first 2 lines from

```dockerfile
FROM debian
# FROM python:3.11-slim
```

to

```dockerfile
# FROM debian
FROM python:3.11-slim
```