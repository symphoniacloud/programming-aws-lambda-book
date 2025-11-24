#!/bin/bash

set -euo pipefail
source harnessShared.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <chapter>"
    exit 1
fi

CHAPTER=$1
STACK_NAME=$(get_stack_name "$CHAPTER")

log_info "Deleting stack: $STACK_NAME"

# For stacks with S3 buckets, we need to empty them first
BUCKETS=$(aws cloudformation list-stack-resources \
    --stack-name "$STACK_NAME" \
    --query "StackResourceSummaries[?ResourceType=='AWS::S3::Bucket'].PhysicalResourceId" \
    --output text 2>/dev/null || echo "")

for bucket in $BUCKETS; do
    if [ -n "$bucket" ]; then
        log_info "Emptying S3 bucket: $bucket"
        aws s3 rm "s3://$bucket" --recursive 2>/dev/null || true
    fi
done

aws cloudformation delete-stack --stack-name "$STACK_NAME"
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME"

log_info "Successfully deleted $STACK_NAME"

cd ../test-harness
