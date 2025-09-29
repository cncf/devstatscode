.PHONY: all build clean test check install help

# Default target
all: build

# Build the project in release mode
build:
	@echo "Building DevStats Rust implementation..."
	cargo build --release
	@mkdir -p bin
	@cp target/release/tags bin/
	@cp target/release/runq bin/
	@cp target/release/health bin/
	@chmod +x bin/*
	@echo "Build completed successfully!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	@rm -rf bin/
	@echo "Clean completed!"

# Run tests
test:
	@echo "Running tests..."
	cargo test
	@echo "Tests completed!"

# Check code without building
check:
	@echo "Checking code..."
	cargo check
	@echo "Check completed!"

# Format code
fmt:
	@echo "Formatting code..."
	cargo fmt
	@echo "Format completed!"

# Lint code
clippy:
	@echo "Running clippy..."
	cargo clippy -- -D warnings
	@echo "Clippy completed!"

# Install dependencies and build
install: build
	@echo "DevStats Rust implementation built successfully!"
	@echo "Binaries available in bin/ directory:"
	@ls -la bin/

# Show help
help:
	@echo "DevStats Rust Implementation - Available targets:"
	@echo ""
	@echo "  all     - Build the project (default)"
	@echo "  build   - Build the project in release mode"
	@echo "  clean   - Clean build artifacts"
	@echo "  test    - Run tests"
	@echo "  check   - Check code without building"
	@echo "  fmt     - Format code"
	@echo "  clippy  - Run clippy linter"
	@echo "  install - Install dependencies and build"
	@echo "  help    - Show this help message"
	@echo ""
	@echo "Example usage:"
	@echo "  make build    # Build the project"
	@echo "  make test     # Run tests"
	@echo "  make clean    # Clean artifacts"