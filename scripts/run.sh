#!/bin/bash

# This script is a convenience wrapper for running the Spark application.
# It ensures that the script is run from the project root

# Change to the directory of the script
cd "$(dirname "$0")/.." || exit

# Paths for data input and output
INPUT_PATH="data/sample/2025-09-13-1.json.gz"
OUTPUT_PATH="data/processed"

# Activate the virtual environment and run the main script
poetry run python src/main.py \
  --job batch \
  --env dev \
  --data-path "$INPUT_PATH" \
  --output-path "$OUTPUT_PATH"