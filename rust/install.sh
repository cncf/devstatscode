#!/bin/bash
# DevStats Rust Installation Script
set -euo pipefail

echo "=== DevStats Rust Installation ==="
echo ""

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: Rust is not installed. Please install Rust first:"
    echo "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

echo "✓ Rust is installed: $(rustc --version)"

# Check for system dependencies
echo "Checking system dependencies..."

# Check for PostgreSQL development libraries
if ! pkg-config --exists libpq; then
    echo "Warning: PostgreSQL development libraries not found."
    echo "On Ubuntu/Debian: sudo apt-get install libpq-dev"
    echo "On CentOS/RHEL: sudo yum install postgresql-devel"
    echo "On macOS: brew install postgresql"
fi

# Check for SQLite development libraries  
if ! pkg-config --exists sqlite3; then
    echo "Warning: SQLite development libraries not found."
    echo "On Ubuntu/Debian: sudo apt-get install libsqlite3-dev"
    echo "On CentOS/RHEL: sudo yum install sqlite-devel"
    echo "On macOS: brew install sqlite"
fi

echo ""
echo "Building DevStats Rust implementation..."
cd "$(dirname "$0")"

# Build the project
if ! make build; then
    echo "Error: Build failed. Please check the error messages above."
    exit 1
fi

echo ""
echo "=== Installation Complete ==="
echo ""
echo "Built binaries:"
ls -la bin/

echo ""
echo "To use the Rust implementation:"
echo "1. Set environment variables (same as Go version):"
echo "   export PG_HOST=localhost"
echo "   export PG_USER=gha_admin"
echo "   export PG_PASS=password"
echo "   export PG_DB=gha"
echo "   export GHA2DB_PROJECT=your_project"
echo ""
echo "2. Run tools:"
echo "   ./bin/health          # Health check"
echo "   ./bin/tags           # Process TSDB tags"
echo "   ./bin/runq file.sql  # Execute SQL queries"
echo ""
echo "3. Optional: Add to PATH:"
echo "   export PATH=\"$(pwd)/bin:\$PATH\""
echo ""
echo "For more information, see rust/README.md"