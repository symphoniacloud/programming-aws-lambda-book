#!/bin/bash
# Teardown all chapter stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../config.sh"

log_info "Tearing down all stacks..."
log_info "Stack prefix: $STACK_PREFIX"

FAILED_TEARDOWNS=()

# Teardown in reverse order (in case of dependencies)
for ((i=${#CHAPTERS[@]}-1; i>=0; i--)); do
    chapter="${CHAPTERS[$i]}"

    log_info "========================================"
    log_info "Tearing down: $chapter"
    log_info "========================================"

    if "$SCRIPT_DIR/teardown-stack.sh" "$chapter"; then
        log_info "Successfully deleted $chapter stack"
    else
        log_error "Failed to delete $chapter stack"
        FAILED_TEARDOWNS+=("$chapter")
    fi

    echo ""
done

if [ ${#FAILED_TEARDOWNS[@]} -gt 0 ]; then
    log_error "Failed teardowns: ${FAILED_TEARDOWNS[*]}"
    exit 1
fi

log_info "All stacks deleted successfully!"
