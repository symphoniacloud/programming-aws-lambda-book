#!/bin/bash
# Teardown a single chapter stack

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

if ! stack_exists "$STACK_NAME"; then
    log_warn "Stack $STACK_NAME does not exist - nothing to delete"
    exit 0
fi

log_info "Deleting stack: $STACK_NAME"

# For stacks with S3 buckets, we need to empty them first
# Get all S3 buckets in the stack
BUCKETS=$(aws cloudformation list-stack-resources \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "StackResourceSummaries[?ResourceType=='AWS::S3::Bucket'].PhysicalResourceId" \
    --output text 2>/dev/null || echo "")

for bucket in $BUCKETS; do
    if [ -n "$bucket" ]; then
        log_info "Emptying S3 bucket: $bucket"
        aws s3 rm "s3://$bucket" --recursive --region "$AWS_REGION" 2>/dev/null || true
    fi
done

# Delete the stack
aws cloudformation delete-stack \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION"

log_info "Waiting for stack deletion to complete..."
aws cloudformation wait stack-delete-complete \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION"

log_info "Successfully deleted $STACK_NAME"
