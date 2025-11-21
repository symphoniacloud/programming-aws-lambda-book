#!/bin/bash
# Deploy all chapter stacks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../config.sh"

log_info "Deploying all chapters..."
log_info "Stack prefix: $STACK_PREFIX"

for chapter in "${CHAPTERS[@]}"; do
    log_info "========================================"
    log_info "Deploying: $chapter"
    log_info "========================================"

    "$SCRIPT_DIR/deploy-stack.sh" "$chapter"
    log_info "Successfully deployed $chapter"

    echo ""
done

log_info "All deployments completed successfully!"
