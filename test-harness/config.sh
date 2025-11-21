#!/bin/bash
# Shared configuration for test harness

# AWS Configuration
export AWS_REGION="us-east-1"
export AWS_DEFAULT_REGION="us-east-1"

# Stack naming
# Use GITHUB_RUN_ID if available (in CI), otherwise use "local"
# Keep prefix short for S3 bucket name limits (63 chars)
export STACK_PREFIX="lb-test-${GITHUB_RUN_ID:-local}"

# Project root (parent of test-harness)
export PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Chapters to test (in order)
export CHAPTERS=(
    "chapter2"
    "chapter3"
    "chapter4"
    "chapter5-api"
    "chapter5-data-pipeline"
    "chapter5-event-sources"
    "chapter6"
    "chapter7"
    "chapter8-s3-errors"
)

# Short names for stacks (S3 bucket names have 63 char limit)
declare -A STACK_SUFFIXES=(
    ["chapter2"]="ch2"
    ["chapter3"]="ch3"
    ["chapter4"]="ch4"
    ["chapter5-api"]="ch5api"
    ["chapter5-data-pipeline"]="ch5dp"
    ["chapter5-event-sources"]="ch5es"
    ["chapter6"]="ch6"
    ["chapter7"]="ch7"
    ["chapter8-s3-errors"]="ch8s3"
)

# Get stack name for a chapter
get_stack_name() {
    local chapter="$1"
    local suffix="${STACK_SUFFIXES[$chapter]:-$chapter}"
    echo "${STACK_PREFIX}-${suffix}"
}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for stack to reach a stable state
wait_for_stack() {
    local stack_name="$1"
    local timeout="${2:-600}"  # Default 10 minutes

    log_info "Waiting for stack $stack_name to stabilize..."

    aws cloudformation wait stack-create-complete \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" 2>/dev/null || \
    aws cloudformation wait stack-update-complete \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" 2>/dev/null

    return $?
}

# Get stack output value
get_stack_output() {
    local stack_name="$1"
    local output_key="$2"

    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue" \
        --output text
}

# Check if stack exists
stack_exists() {
    local stack_name="$1"
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" &>/dev/null
    return $?
}
