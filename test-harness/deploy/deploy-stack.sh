#!/bin/bash
# Deploy a single chapter stack

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
CHAPTER_DIR="$PROJECT_ROOT/$CHAPTER"

if [ ! -d "$CHAPTER_DIR" ]; then
    log_error "Chapter directory not found: $CHAPTER_DIR"
    exit 1
fi

log_info "Deploying $CHAPTER as stack: $STACK_NAME"

cd "$CHAPTER_DIR"

# Build the project
log_info "Building $CHAPTER..."
if [ -f "pom.xml" ]; then
    # Check if it's a multi-module project
    if [ -d "bulk-events-stage" ] || [ -d "single-event-stage" ]; then
        mvn clean package -DskipTests -q
    else
        mvn clean package -DskipTests -q
    fi
else
    log_error "No pom.xml found in $CHAPTER_DIR"
    exit 1
fi

# Deploy with SAM
log_info "Deploying with SAM..."
sam deploy \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --resolve-s3 \
    --capabilities CAPABILITY_IAM \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset

log_info "Successfully deployed $STACK_NAME"

# Show outputs if any
OUTPUTS=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs' \
    --output table 2>/dev/null || echo "No outputs")

if [ "$OUTPUTS" != "No outputs" ] && [ -n "$OUTPUTS" ]; then
    log_info "Stack outputs:"
    echo "$OUTPUTS"
fi
