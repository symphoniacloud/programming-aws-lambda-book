#!/bin/bash

set -euo pipefail
source harnessShared.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <chapter>"
    exit 1
fi

CHAPTER=$1
STACK_NAME=$(get_stack_name "$CHAPTER")
TEST_SCRIPT="tests/${CHAPTER}.sh"

if [ ! -f "$TEST_SCRIPT" ]; then
    log_warn "No test script found for $CHAPTER at $TEST_SCRIPT"
    exit 0
fi

if ! stack_exists "$STACK_NAME"; then
    log_error "Stack $STACK_NAME does not exist. Deploy it first."
    exit 1
fi

log_info "Running smoke test for $CHAPTER (stack: $STACK_NAME)"

export STACK_NAME

if bash "$TEST_SCRIPT"; then
    log_info "PASS: $CHAPTER"
    exit 0
else
    log_error "FAIL: $CHAPTER"
    exit 1
fi
