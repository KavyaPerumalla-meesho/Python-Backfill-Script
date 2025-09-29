#!/bin/bash

# ScyllaDB Backfill Script Runner
# This script activates the virtual environment and runs the backfill process

echo "🚀 Starting ScyllaDB Backfill Process"
echo "======================================"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run setup first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Run the backfill script
python proto_backfill_main.py "$@"

echo "✅ Backfill process completed!"
