#!/usr/bin/env bash
set -euo pipefail

if ! command -v mvn >/dev/null 2>&1; then
  echo "Maven is required but not found in PATH."
  exit 1
fi

echo "Using Maven: $(mvn -v | head -n 1)"
echo "Running clean test package..."
mvn -q clean test package

echo "Build complete. Shaded artifact should be in target/*.jar"
