#!/bin/bash

set -euo pipefail
source harnessShared.sh

for CHAPTER in "${CHAPTERS[@]}"; do
  ./deploy-stack.sh "$CHAPTER"
done
