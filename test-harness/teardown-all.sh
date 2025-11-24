#!/bin/bash
# Teardown all chapter stacks

set -euo pipefail
source harnessShared.sh

for CHAPTER in "${CHAPTERS[@]}"; do
  ./teardown-stack.sh "$CHAPTER"
done
