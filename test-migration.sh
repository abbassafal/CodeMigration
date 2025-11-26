#!/bin/bash

# Material Master Migration - Performance Test Script
# This script demonstrates the performance improvement of the optimized migration

echo "🚀 Material Master Migration - Performance Test"
echo "=============================================="
echo

# Base URL - adjust if needed
BASE_URL="http://localhost:5000"

echo "📊 Step 1: Getting migration time estimate..."
curl -s -X GET "$BASE_URL/Migration/material/estimate-time" | jq '.'
echo

echo "⏱️  Step 2: Starting optimized migration with console progress..."
echo "This will show real-time progress in the server console."
echo "Press Ctrl+C to cancel if needed."
echo

# Start the migration
START_TIME=$(date +%s)
RESPONSE=$(curl -s -X POST "$BASE_URL/Migration/material/migrate-with-console-progress")
END_TIME=$(date +%s)

# Calculate duration
DURATION=$((END_TIME - START_TIME))

echo "📈 Migration Response:"
echo $RESPONSE | jq '.'
echo

echo "⏱️  Total API Response Time: ${DURATION} seconds"
echo

# Extract record count if successful
SUCCESS=$(echo $RESPONSE | jq -r '.success')
if [ "$SUCCESS" = "true" ]; then
    RECORD_COUNT=$(echo $RESPONSE | jq -r '.recordCount')
    echo "✅ Migration completed successfully!"
    echo "📊 Records migrated: $RECORD_COUNT"
    
    if [ $DURATION -gt 0 ]; then
        RATE=$((RECORD_COUNT / DURATION))
        echo "🚀 Average rate: ~$RATE records/second"
    fi
else
    ERROR=$(echo $RESPONSE | jq -r '.error')
    echo "❌ Migration failed: $ERROR"
fi

echo
echo "🌐 For real-time progress monitoring, visit:"
echo "   $BASE_URL/Migration/material/progress-dashboard"
echo
echo "🔄 To start real-time migration, use:"
echo "   curl -X POST '$BASE_URL/Migration/material/migrate-optimized'"
echo
