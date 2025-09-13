#!/bin/bash

# This script is a convenience wrapper for running the Spark application.
# It ensures that the script is run from the project root

# Change to the directory of the script
cd "$(dirname "$0")/.."

# Activate the virtual environment and run the main script
poetry run python src/main.py --job exploration --env dev