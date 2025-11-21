#!/bin/bash
# Run smoke test for a single chapter stack

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../config.sh"

if [ -z "$1" ]; then
    echo "Usage: $0 <chapter>"
    echo "Available chapters: ${CHAPTERS[*]}"
    exit 1
fi

CHAPTER="$1"
STACK_NAME=$(get_stack_name "$CHAPTER")
TEST_SCRIPT="$SCRIPT_DIR/tests/${CHAPTER}.sh"

if [ ! -f "$TEST_SCRIPT" ]; then
    log_warn "No test script found for $CHAPTER at $TEST_SCRIPT"
    exit 0
fi

# Check if stack exists
if ! stack_exists "$STACK_NAME"; then
    log_error "Stack $STACK_NAME does not exist. Deploy it first."
    exit 1
fi

log_info "Running smoke test for $CHAPTER (stack: $STACK_NAME)"

# Export stack name for test script
export STACK_NAME

# Run the test
if bash "$TEST_SCRIPT"; then
    log_info "PASS: $CHAPTER"
    exit 0
else
    log_error "FAIL: $CHAPTER"
    exit 1
fi
