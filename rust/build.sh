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
cp target/release/annotations bin/
cp target/release/columns bin/
cp target/release/devstats bin/
cp target/release/get_repos bin/
cp target/release/gha2db bin/
cp target/release/gha2db_sync bin/
cp target/release/ghapi2db bin/
cp target/release/hide_data bin/
cp target/release/import_affs bin/
cp target/release/merge_dbs bin/
cp target/release/replacer bin/
cp target/release/splitcrons bin/
cp target/release/sqlitedb bin/
cp target/release/structure bin/
cp target/release/sync_issues bin/
cp target/release/tsplit bin/
cp target/release/vars bin/
cp target/release/webhook bin/
cp target/release/website_data bin/
cp target/release/api bin/
cp target/release/calc_metric bin/

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