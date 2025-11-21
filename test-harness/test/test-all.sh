#!/bin/bash
# Run smoke tests for all chapter stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../config.sh"

log_info "Running all smoke tests..."
log_info "Stack prefix: $STACK_PREFIX"

for chapter in "${CHAPTERS[@]}"; do
    log_info "========================================"
    log_info "Testing: $chapter"
    log_info "========================================"

    TEST_SCRIPT="$SCRIPT_DIR/tests/${chapter}.sh"

    if [ ! -f "$TEST_SCRIPT" ]; then
        log_warn "No test script found for $chapter - skipping"
        continue
    fi

    "$SCRIPT_DIR/test-stack.sh" "$chapter"
    log_info "PASS: $chapter"

    echo ""
done

log_info "All tests passed!"
