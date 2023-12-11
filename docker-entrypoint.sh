#!/bin/ash
set -e

# Set permissions
user="$(id -u)" # Check if user is root
if [ "$user" = '0' ]; then
        [ -d "/mosquitto" ] && chown -R mosquitto:mosquitto /mosquitto || true
fi

# Start logging script
python3 /app/main.py &

exec "$@"