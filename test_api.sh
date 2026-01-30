#!/bin/bash
# Test API endpoints

echo "Testing backend API..."
echo ""

echo "1. Health check:"
curl -s http://localhost:3001/health | python3 -m json.tool
echo ""

echo "2. Get alarms:"
curl -s http://localhost:3001/api/alarms | python3 -m json.tool | head -20
echo ""

echo "2b. Clear alarms (archive all):"
curl -s -X POST "http://localhost:3001/api/alarms/clear?action=archive" | python3 -m json.tool
echo ""

echo "2c. Get alarms (should now be empty unless include_archived=true):"
curl -s http://localhost:3001/api/alarms | python3 -m json.tool | head -20
echo ""

echo "3. Get thresholds:"
curl -s http://localhost:3001/api/thresholds | python3 -m json.tool | head -20
echo ""

echo "4. Get power flow:"
curl -s http://localhost:3001/api/power-flow | python3 -m json.tool
echo ""

echo "5. Get energy mix:"
curl -s http://localhost:3001/api/energy-mix | python3 -m json.tool | head -30
echo ""

echo "✅ All tests complete!"
