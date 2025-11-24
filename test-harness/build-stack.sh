#!/bin/bash

set -euo pipefail
source harnessShared.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <chapter>"
    exit 1
fi

CHAPTER=$1

log_info "Building: $CHAPTER"
CHAPTER_DIR="../$CHAPTER"
if [ ! -d "$CHAPTER_DIR" ]; then
    log_error "Chapter directory not found: $CHAPTER_DIR"
    exit 1
fi
cd "$CHAPTER_DIR"
mvn clean package
cd ../test-harness