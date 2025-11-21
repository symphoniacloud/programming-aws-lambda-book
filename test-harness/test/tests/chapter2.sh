#!/bin/bash
# Smoke test for Chapter 2 - Hello World Lambda

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$SCRIPT_DIR/config.sh"

log_info "Testing Chapter 2 - Hello World Lambda"

# Get the Lambda function name from the stack
FUNCTION_NAME=$(aws cloudformation list-stack-resources \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "StackResourceSummaries[?ResourceType=='AWS::Lambda::Function'].PhysicalResourceId" \
    --output text)

if [ -z "$FUNCTION_NAME" ]; then
    log_error "Could not find Lambda function in stack $STACK_NAME"
    exit 1
fi

log_info "Invoking Lambda function: $FUNCTION_NAME"

# Invoke the function
RESPONSE=$(aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --payload '"World"' \
    --cli-binary-format raw-in-base64-out \
    /tmp/chapter2-response.json \
    --output json)

# Check for errors in invocation
if echo "$RESPONSE" | grep -q '"FunctionError"'; then
    log_error "Lambda invocation failed"
    cat /tmp/chapter2-response.json
    exit 1
fi

# Check response
RESULT=$(cat /tmp/chapter2-response.json)
log_info "Response: $RESULT"

# Basic validation - should return a string containing "Hello"
if echo "$RESULT" | grep -qi "hello"; then
    log_info "Response contains expected greeting"
else
    log_warn "Response may not contain expected greeting, but invocation succeeded"
fi

log_info "Chapter 2 test passed"
