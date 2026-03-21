#!/bin/bash
# Quick start deployment script for Cohort Analysis

set -e

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  Monthly Customer Retention Cohort Analysis - Quick Start Deploy   ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "✅ Docker and Docker Compose detected"
echo ""

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "📋 Creating .env from template..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✅ .env created. Please edit with your Google Sheets IDs and credentials path"
        echo ""
        echo "Required values to fill in .env:"
        echo "  - INPUT_SHEET_ID"
        echo "  - OUTPUT_SHEET_ID"
        echo "  - GOOGLE_CREDENTIALS_FILE (usually /app/credentials.json)"
        echo ""
        exit 1
    else
        echo "❌ .env.example not found"
        exit 1
    fi
fi

# Check if credentials file exists
CREDS_FILE=$(grep GOOGLE_CREDENTIALS_FILE .env | cut -d= -f2 | xargs)
CREDS_LOCAL="${CREDS_FILE##*/}"  # Get filename only

if [ ! -f "$CREDS_LOCAL" ]; then
    echo "❌ Credentials file not found: $CREDS_LOCAL"
    echo ""
    echo "Please:"
    echo "  1. Download your Google Service Account JSON from Google Cloud Console"
    echo "  2. Save it as: $CREDS_LOCAL"
    echo "  3. Run this script again"
    exit 1
fi

echo "✅ Credentials file found"
echo ""

# Test Docker build
echo "🔨 Building Docker image..."
docker-compose build

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "✅ Setup Complete! Starting container..."
echo "════════════════════════════════════════════════════════════════════"
echo ""

# Start the service
docker-compose up -d

echo ""
echo "✅ Container started in background"
echo ""
echo "📊 Next Steps:"
echo "  • View logs: docker-compose logs -f"
echo "  • Stop service: docker-compose down"
echo "  • Test manually: docker-compose run --profile manual cohort-analysis-once"
echo ""
echo "⏰ Analysis will run daily at 7:00 AM UTC"
echo ""
