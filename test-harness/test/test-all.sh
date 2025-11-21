#!/bin/bash
# Run smoke tests for all chapter stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../config.sh"

log_info "Running all smoke tests..."
log_info "Stack prefix: $STACK_PREFIX"

PASSED=()
FAILED=()
SKIPPED=()

for chapter in "${CHAPTERS[@]}"; do
    log_info "========================================"
    log_info "Testing: $chapter"
    log_info "========================================"

    TEST_SCRIPT="$SCRIPT_DIR/tests/${chapter}.sh"

    if [ ! -f "$TEST_SCRIPT" ]; then
        log_warn "No test script found for $chapter - skipping"
        SKIPPED+=("$chapter")
        continue
    fi

    if "$SCRIPT_DIR/test-stack.sh" "$chapter"; then
        PASSED+=("$chapter")
    else
        FAILED+=("$chapter")
    fi

    echo ""
done

# Summary
echo ""
log_info "========================================"
log_info "TEST SUMMARY"
log_info "========================================"
log_info "Passed: ${#PASSED[@]} - ${PASSED[*]:-none}"
log_warn "Skipped: ${#SKIPPED[@]} - ${SKIPPED[*]:-none}"
log_error "Failed: ${#FAILED[@]} - ${FAILED[*]:-none}"

if [ ${#FAILED[@]} -gt 0 ]; then
    exit 1
fi

log_info "All tests passed!"
