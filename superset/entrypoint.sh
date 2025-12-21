#!/bin/bash
set -e

# Create an admin user if it doesn't exist
superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname Superset \
    --lastname Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD"

# Upgrade the database
superset db upgrade

# Initialize Superse
superset init

# /bin/sh -c /usr/bin/run-server.sh
# superset run -p 8088 --with-threads --reload

gunicorn --bind 0.0.0.0:8088 "superset.app:create_app()"
