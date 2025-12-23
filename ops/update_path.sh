#!/usr/bin/env bash

set -e

ABS_PATH="$(pwd)"
CONFIG_FILE="configs/base.yaml"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: $CONFIG_FILE not found!"
    exit 1
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s|/ABS/PATH/brazillian-ecommerce-analytics|${ABS_PATH}|g" "$CONFIG_FILE"
else
    sed -i "s|/ABS/PATH/brazillian-ecommerce-analytics|${ABS_PATH}|g" "$CONFIG_FILE"
fi

echo "✓ Successfully updated $CONFIG_FILE with ABS_PATH=${ABS_PATH}"
