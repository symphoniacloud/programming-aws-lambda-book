#!/bin/bash

set -euo pipefail
source "./config.sh"

echo $CHAPTERS

for CHAPTER in "${CHAPTERS[@]}"; do
  log_info "Building: $CHAPTER"
  CHAPTER_DIR="../$CHAPTER"
  if [ ! -d "$CHAPTER_DIR" ]; then
      log_error "Chapter directory not found: $CHAPTER_DIR"
      exit 1
  fi
  cd $CHAPTER_DIR
  mvn clean package
  cd ../test-harness
#
#    "$SCRIPT_DIR/deploy-stack.sh" "$chapter"
#    log_info "Successfully deployed $chapter"
#
#    echo ""
done