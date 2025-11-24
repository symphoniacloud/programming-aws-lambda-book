#!/bin/bash

set -euo pipefail
source harnessShared.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <chapter>"
    exit 1
fi

CHAPTER=$1
STACK_NAME=$(get_stack_name "$CHAPTER")

log_info "Deploying $CHAPTER as stack: $STACK_NAME"

CHAPTER_DIR="../$CHAPTER"
if [ ! -d "$CHAPTER_DIR" ]; then
    log_error "Chapter directory not found: $CHAPTER_DIR"
    exit 1
fi
log_info "Building $CHAPTER..."
cd "$CHAPTER_DIR"
mvn clean package

log_info "Deploying..."
sam deploy \
    --stack-name "$STACK_NAME" \
    --resolve-s3 \
    --capabilities CAPABILITY_IAM \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset

log_info "Successfully deployed $STACK_NAME"

cd ../test-harness
