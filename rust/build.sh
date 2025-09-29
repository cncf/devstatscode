#!/bin/bash
set -euo pipefail

# Build script for DevStats Rust implementation
echo "Building DevStats Rust implementation..."

# Build in release mode for optimal performance
cargo build --release

# Create bin directory if it doesn't exist
mkdir -p bin

# Copy binaries to bin directory
cp target/release/tags bin/
cp target/release/runq bin/
cp target/release/health bin/

# Make binaries executable
chmod +x bin/*

echo "Build completed successfully!"
echo "Binaries available in bin/ directory:"
ls -la bin/

# Run basic tests to ensure binaries work
echo ""
echo "Running basic tests..."

echo "Testing tags command:"
./bin/tags --help || echo "tags help failed (expected if no help implemented)"

echo "Testing health command:"
./bin/health --help || echo "health help failed (expected if no help implemented)"

echo ""
echo "Build and test completed!"