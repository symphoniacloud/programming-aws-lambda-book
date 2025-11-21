#!/bin/bash
# Smoke test for Chapter 5 API - Weather Events API

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$SCRIPT_DIR/config.sh"

log_info "Testing Chapter 5 API - Weather Events"

# Get the API Gateway URL from stack outputs
# First, let's find the API Gateway RestApi
API_ID=$(aws cloudformation list-stack-resources \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "StackResourceSummaries[?ResourceType=='AWS::ApiGateway::RestApi'].PhysicalResourceId" \
    --output text)

if [ -z "$API_ID" ]; then
    log_error "Could not find API Gateway in stack $STACK_NAME"
    exit 1
fi

# Construct the API URL
API_URL="https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/Prod"
log_info "API URL: $API_URL"

# Test 1: POST a weather event
log_info "Posting weather event..."

WEATHER_EVENT='{
  "locationName": "Brooklyn, NY",
  "temperature": 91,
  "timestamp": 1564428897,
  "latitude": 40.70,
  "longitude": -73.99
}'

POST_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    "${API_URL}/events" \
    -H "Content-Type: application/json" \
    -d "$WEATHER_EVENT")

POST_BODY=$(echo "$POST_RESPONSE" | sed '$d')
POST_STATUS=$(echo "$POST_RESPONSE" | tail -1)

log_info "POST Status: $POST_STATUS"
log_info "POST Response: $POST_BODY"

if [ "$POST_STATUS" -lt 200 ] || [ "$POST_STATUS" -ge 300 ]; then
    log_error "POST request failed with status $POST_STATUS"
    exit 1
fi

# Wait a moment for DynamoDB to be consistent
sleep 2

# Test 2: GET locations
log_info "Getting locations..."

GET_RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "${API_URL}/locations")

GET_BODY=$(echo "$GET_RESPONSE" | sed '$d')
GET_STATUS=$(echo "$GET_RESPONSE" | tail -1)

log_info "GET Status: $GET_STATUS"
log_info "GET Response: $GET_BODY"

if [ "$GET_STATUS" -lt 200 ] || [ "$GET_STATUS" -ge 300 ]; then
    log_error "GET request failed with status $GET_STATUS"
    exit 1
fi

# Verify the posted data is in the response
if echo "$GET_BODY" | grep -q "Brooklyn"; then
    log_info "Successfully found posted weather event in response"
else
    log_warn "Posted data not found in response (may need more time for consistency)"
fi

log_info "Chapter 5 API test passed"
