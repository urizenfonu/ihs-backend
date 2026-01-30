#!/bin/bash
# Start the IHS Alarm Monitor backend

echo "🚀 Starting IHS Alarm Monitor Backend..."
echo "📍 Running on http://localhost:3001"
echo ""

./venv/bin/uvicorn main:app --reload --port 3002 --host 0.0.0.0
