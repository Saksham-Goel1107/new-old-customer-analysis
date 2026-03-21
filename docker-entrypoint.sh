#!/bin/bash
# Docker entrypoint script with validation

set -e

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  Cohort Analysis Container Starting                               ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Check required environment variables
echo "🔍 Validating configuration..."

if [ -z "$INPUT_SHEET_ID" ]; then
    echo "❌ Error: INPUT_SHEET_ID environment variable not set"
    exit 1
fi

if [ -z "$OUTPUT_SHEET_ID" ]; then
    echo "❌ Error: OUTPUT_SHEET_ID environment variable not set"
    exit 1
fi

if [ ! -f "$GOOGLE_CREDENTIALS_FILE" ]; then
    echo "❌ Error: Credentials file not found at $GOOGLE_CREDENTIALS_FILE"
    exit 1
fi

# Validate JSON credentials
if ! python -c "import json; json.load(open('$GOOGLE_CREDENTIALS_FILE'))" 2>/dev/null; then
    echo "❌ Error: Invalid JSON in credentials file"
    exit 1
fi

echo "✅ Configuration valid"
echo ""

# Set run mode
RUN_MODE=${RUN_MODE:-scheduled}
echo "ℹ️  Running in: $RUN_MODE mode"

if [ "$RUN_MODE" = "scheduled" ]; then
    echo "⏰ Analysis will execute daily at 07:00 UTC"
    echo ""
else
    echo "▶️  Running analysis immediately..."
    echo ""
fi

# Start application
exec python app.py
