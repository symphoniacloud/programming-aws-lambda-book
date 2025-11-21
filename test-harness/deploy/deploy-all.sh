#!/bin/bash
# Deploy all chapter stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../config.sh"

log_info "Deploying all chapters..."
log_info "Stack prefix: $STACK_PREFIX"

FAILED_DEPLOYS=()

for chapter in "${CHAPTERS[@]}"; do
    log_info "========================================"
    log_info "Deploying: $chapter"
    log_info "========================================"

    if "$SCRIPT_DIR/deploy-stack.sh" "$chapter"; then
        log_info "Successfully deployed $chapter"
    else
        log_error "Failed to deploy $chapter"
        FAILED_DEPLOYS+=("$chapter")
    fi

    echo ""
done

if [ ${#FAILED_DEPLOYS[@]} -gt 0 ]; then
    log_error "Failed deployments: ${FAILED_DEPLOYS[*]}"
    exit 1
fi

log_info "All deployments completed successfully!"
