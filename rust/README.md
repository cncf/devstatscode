# DevStats Rust Implementation

This directory contains a complete rewrite of the DevStats system in Rust, providing the same functionality as the original Go implementation with improved performance, memory safety, and modern async/await patterns.

## Structure

```
rust/
├── Cargo.toml              # Workspace configuration
├── Makefile                # Build automation
├── build.sh                # Build script
├── README.md               # This file
├── devstats-core/          # Core library with shared functionality
│   ├── src/
│   │   ├── constants.rs    # Constants (translated from const.go)
│   │   ├── context.rs      # Context struct (translated from context.go)
│   │   ├── error.rs        # Error types
│   │   └── lib.rs          # Library entry point
│   └── Cargo.toml
├── devstats-cli/           # Command-line tools
│   ├── src/
│   │   ├── tags.rs         # Tags command (translated from cmd/tags/tags.go)
│   │   └── runq.rs         # RunQ command (translated from cmd/runq/runq.go)
│   └── Cargo.toml
└── bin/                    # Built binaries (created after build)
    ├── tags
    └── runq
```

## Features

### Core Library (`devstats-core`)

- **Context Management**: Environment variable parsing and configuration management
- **Database Connectivity**: PostgreSQL and SQLite support via SQLx
- **Error Handling**: Comprehensive error types with proper error chaining
- **Constants**: All system constants translated from Go
- **Async/Await**: Modern async patterns for I/O operations

### CLI Tools (`devstats-cli`)

#### `tags` - TSDB Tags Tool
- Reads YAML configuration files
- Processes database queries for tag generation  
- Supports all original environment variables
- Multi-threaded processing (when enabled)

#### `runq` - SQL Query Runner
- Executes SQL files with parameter replacement
- Supports all original parameter formats including `readfile:` syntax
- Tab-separated output format
- EXPLAIN query support

## Building

### Prerequisites

- Rust 1.70+ (latest stable recommended)
- PostgreSQL development libraries (for SQLx)
- SQLite development libraries (for SQLx)

### Build Commands

```bash
# Build everything
make build

# Or use the build script
./build.sh

# Or use Cargo directly
cargo build --release

# Run tests
make test

# Format code
make fmt

# Run linter
make clippy

# Clean build artifacts
make clean
```

## Usage

The Rust tools are designed to be drop-in replacements for the Go versions:

### Environment Variables

All original environment variables are supported:

- `GHA2DB_*` - General configuration
- `PG_*` - PostgreSQL connection settings  
- Project and path configurations
- Debug and logging settings

### Examples

```bash
# Run tags for a specific project
export GHA2DB_PROJECT=kubernetes
./bin/tags

# Execute a SQL query with parameters  
./bin/runq queries/example.sql "{{param1}}" "value1" "{{param2}}" "value2"

# Use readfile parameter replacement
./bin/runq queries/complex.sql "{{list}}" "readfile:data/items.txt"

# Enable debug output
export GHA2DB_DEBUG=2
export GHA2DB_QOUT=1
./bin/tags
```

## Configuration

Configuration works exactly like the Go version:

- Environment variables for runtime configuration
- YAML files for metrics, tags, and other structured data
- SQL files for queries and data processing
- Same file paths and naming conventions

## Performance

The Rust implementation provides several performance benefits:

- **Memory Safety**: No garbage collection overhead
- **Async I/O**: Non-blocking database and file operations
- **Zero-Copy**: Efficient string handling where possible
- **Compile-Time Optimization**: Extensive compiler optimizations

## Compatibility

The Rust implementation is designed to be 100% compatible with the existing Go version:

- Same command-line interface
- Same configuration files and formats
- Same environment variables
- Same output formats
- Same database schema expectations

## Development

### Adding New Commands

1. Create a new binary in `devstats-cli/src/`
2. Add the binary to `devstats-cli/Cargo.toml` 
3. Update the build script to copy the new binary
4. Follow the patterns established in existing commands

### Core Library Extensions

Add new functionality to `devstats-core` and expose it via `lib.rs`. All CLI tools automatically get access to core library functions.

## Migration Strategy

This Rust implementation allows for gradual migration:

1. **Preserve Original**: The original Go code remains untouched in the parent directory
2. **Side-by-Side**: Both implementations can coexist
3. **Drop-In Replacement**: Rust binaries can replace Go binaries when ready
4. **Incremental**: Migrate commands one at a time as needed

## Future Plans

The Rust implementation provides a foundation for future enhancements:

- GraphQL API server
- Real-time metrics streaming
- Enhanced monitoring and observability
- Cloud-native deployment improvements
- Additional database backends

## Contributing

When contributing to the Rust implementation:

1. Follow Rust naming conventions and idioms
2. Maintain compatibility with the Go version
3. Add tests for new functionality
4. Run `cargo fmt` and `cargo clippy` before committing
5. Update this README when adding new features

## License

Same license as the parent DevStats project: Apache-2.0