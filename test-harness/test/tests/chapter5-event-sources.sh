#!/bin/bash
# Smoke test for Chapter 5 Event Sources - API Gateway example

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$SCRIPT_DIR/config.sh"

log_info "Testing Chapter 5 Event Sources - API Gateway"

# Get the API Gateway RestApi
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

# Test: GET /foo
log_info "Testing GET /foo..."

GET_RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "${API_URL}/foo")

GET_BODY=$(echo "$GET_RESPONSE" | sed '$d')
GET_STATUS=$(echo "$GET_RESPONSE" | tail -1)

log_info "GET Status: $GET_STATUS"
log_info "GET Response: $GET_BODY"

if [ "$GET_STATUS" -lt 200 ] || [ "$GET_STATUS" -ge 300 ]; then
    log_error "GET request failed with status $GET_STATUS"
    exit 1
fi

log_info "Chapter 5 Event Sources test passed"
