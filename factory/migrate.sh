#!/bin/bash

# Migration script to copy table data from source to target database using pgcopydb
# This script is designed to be run non-interactively, e.g., via cron job

# Set variables
SOURCE_DB=""
TARGET_DB=""
LOG_FILE="~/pycoffeemaker/.logs/migration.log"

# Clear any previous pgcopydb work directory to ensure fresh start
rm -rf /tmp/pgcopydb

# Run pgcopydb with options to force new snapshot and overwrite data
pgcopydb copy table-data --not-consistent \
    --source "$SOURCE_DB" \
    --target "$TARGET_DB" \
    >> "$LOG_FILE" 2>&1

# Check exit code
if [ $? -eq 0 ]; then
    echo "$(date): Migration successful" >> "$LOG_FILE"
else
    echo "$(date): Migration failed with exit code $?" >> "$LOG_FILE"
    # Optionally send notification, e.g., email or webhook
fi