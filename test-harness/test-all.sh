#!/bin/bash

set -euo pipefail
source harnessShared.sh

for CHAPTER in "${CHAPTERS[@]}"; do
  ./test-stack.sh "$CHAPTER"
done
