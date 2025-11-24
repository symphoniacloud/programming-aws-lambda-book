#!/bin/bash

set -euo pipefail
source harnessShared.sh

for CHAPTER in "${CHAPTERS[@]}"; do
  ./build-stack.sh "$CHAPTER"
done