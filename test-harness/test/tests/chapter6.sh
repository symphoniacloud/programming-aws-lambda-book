#!/bin/bash
# Smoke test for Chapter 6 - Data Pipeline with Integration Tests
#
# This runs the Maven integration tests that are part of chapter6.
# These tests deploy to and test against the actual AWS stack.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$SCRIPT_DIR/config.sh"

log_info "Testing Chapter 6 - Running Maven Integration Tests"

CHAPTER_DIR="$PROJECT_ROOT/chapter6"

if [ ! -d "$CHAPTER_DIR" ]; then
    log_error "Chapter directory not found: $CHAPTER_DIR"
    exit 1
fi

cd "$CHAPTER_DIR"

log_info "Running unit tests and installing dependencies"
# Run unit tests during install
mvn install -Dexec.skip=true -q

log_info "Running integration tests against stack: $STACK_NAME"
# Skip exec plugin (it tries to run sam deploy, but we already deployed)
mvn -pl integration-tests verify -DstackName="$STACK_NAME" -DskipTests -Dexec.skip=true -q

log_info "Chapter 6 tests passed"
