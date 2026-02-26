# Fletch

A command-line tool for transferring data between databases using Apache Arrow Database Connectivity (ADBC). Supports both interactive and non-interactive (AI agent-friendly) modes.

## Features

- **Multi-Database Support**: Connect to PostgreSQL, SQLite, DuckDB, BigQuery, MotherDuck, and custom ADBC drivers
- **Flexible Authentication**: Support for service accounts, API tokens, environment variables, and Application Default Credentials
- **Automatic Driver Installation**: Detects missing ADBC drivers and offers to install them automatically
- **Batch Transfer Mode**: Process all data at once with transaction support (ideal for smaller datasets)
- **Streaming Transfer Mode**: Process data in chunks using efficient BindStream (memory-efficient for large datasets)
- **Flexible Table Management**: Create new tables, append to existing, or replace tables
- **Full Data Insertion**: Complete implementation with ADBC bulk ingestion support
- **Transaction Support**: Automatic rollback on errors for data integrity
- **Interactive & Non-Interactive Modes**: Wizard-style prompts for humans, CLI flags for scripts and AI agents
- **JSON Output**: Machine-readable output for programmatic consumption
- **Dry Run**: Validate transfer configuration without executing
- **Distinct Exit Codes**: Granular error codes for automated error handling
- **High Performance**: Built on Apache Arrow for zero-copy data transfer

## Installation

### Prerequisites

- Go 1.25 or later
- ADBC drivers for your databases (can be installed automatically by the CLI)

### Build from Source

```bash
git clone <your-repo>
cd go-test
go build -o fletch
```

### ADBC Driver Installation

The CLI will **automatically detect** if ADBC drivers are missing and offer to install them for you using the best method available:

**macOS** (Intelligent 3-tier approach):
1. **Priority 1**: Uses `dbc` if already installed (fastest, most lightweight)
2. **Priority 2**: Offers to install `dbc` first (recommended - targeted driver installation)
3. **Priority 3**: Falls back to `apache-arrow` via Homebrew (installs all drivers)

**What is dbc?**
`dbc` is Columnar Technologies' database connectivity tool that provides:
- **Fast installation** - Only installs the specific driver you need
- **Lightweight** - No need for the entire Apache Arrow suite
- **Targeted** - `dbc install sqlite`, `dbc install postgresql`, etc.
- **Reusable** - Install additional drivers easily

**Linux**:
- The CLI will provide instructions for your package manager
- Ubuntu/Debian: `sudo apt install apache-arrow-adbc`
- Fedora/RHEL: `sudo dnf install apache-arrow-adbc`

**Windows**:
- The CLI will provide instructions for vcpkg or building from source

**Manual Installation** (Optional):
If you prefer to install drivers before running the CLI:

```bash
# macOS - Recommended (dbc)
brew tap columnar-tech/tap && brew install --cask dbc
dbc install sqlite
dbc install postgresql
dbc install duckdb

# macOS - Alternative (Apache Arrow - all drivers)
brew install apache-arrow

# Ubuntu/Debian
sudo apt install apache-arrow-adbc

# Or build from source
git clone https://github.com/apache/arrow-adbc.git
cd arrow-adbc
# Follow build instructions
```

## Usage

### Subcommands

| Command | Description |
|---------|-------------|
| `transfer` | Transfer data between databases |
| `test-connection` | Validate connectivity to a database |
| `list-drivers` | List supported database drivers |
| `version` | Print version information |

### Global Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--output` | `-o` | Output format: `text` (default), `json`, `quiet` |
| `--help` | `-h` | Show help |

---

### `transfer` - Transfer Data Between Databases

When run without flags, starts an interactive wizard. When all required flags are provided, runs non-interactively.

#### Transfer Flags

| Flag | Description | Required | Default |
|------|-------------|----------|---------|
| `--source-driver` | Source database driver | Yes | |
| `--source-uri` | Source connection URI | Yes | |
| `--dest-driver` | Destination database driver | Yes | |
| `--dest-uri` | Destination connection URI | Yes | |
| `--dest-table` | Destination table name | Yes | |
| `--query` | SQL query to execute on source | Yes* | |
| `--query-file` | Path to SQL file (use `-` for stdin) | Yes* | |
| `--ingest-mode` | Table ingest mode: `create`, `append`, `replace` | No | `create` |
| `--transfer-mode` | Transfer mode: `batch`, `streaming` | No | `batch` |
| `--yes`, `-y` | Skip confirmation prompt | No | `false` |
| `--dry-run` | Validate inputs without executing | No | `false` |
| `--interactive` | Force interactive wizard mode | No | `false` |
| `--auto-install-drivers` | Auto-install missing drivers | No | `false` |
| `--no-install-drivers` | Fail immediately if drivers missing | No | `false` |

*Either `--query` or `--query-file` is required, but not both.

#### Non-Interactive Mode (AI Agent / Script Friendly)

```bash
fletch transfer \
  --source-driver postgresql \
  --source-uri "postgresql://user:pass@localhost:5432/mydb" \
  --dest-driver duckdb \
  --dest-uri "/tmp/output.duckdb" \
  --dest-table orders_backup \
  --ingest-mode create \
  --transfer-mode streaming \
  --query "SELECT * FROM orders WHERE year = 2025" \
  --yes
```

#### With JSON Output

```bash
fletch transfer \
  --source-driver sqlite --source-uri "file:data.db" \
  --dest-driver duckdb --dest-uri ":memory:" \
  --dest-table export \
  --query "SELECT * FROM users" \
  --yes --output json
```

JSON success output:
```json
{
  "status": "success",
  "rows_transferred": 2847,
  "batches": 3,
  "source": { "driver": "postgresql", "name": "postgresql" },
  "destination": { "driver": "duckdb", "name": "duckdb" },
  "table": "orders_backup",
  "ingest_mode": "adbc.ingest.mode.create",
  "transfer_mode": "streaming",
  "query": "SELECT * FROM orders WHERE year = 2025",
  "duration_ms": 1234
}
```

JSON error output:
```json
{
  "error": "missing required flags: --source-uri, --dest-driver",
  "error_code": 2
}
```

#### Read Query from File

```bash
fletch transfer \
  --source-driver postgresql --source-uri "postgresql://..." \
  --dest-driver duckdb --dest-uri "output.duckdb" \
  --dest-table results \
  --query-file query.sql \
  --yes
```

Or pipe from stdin:
```bash
echo "SELECT * FROM orders" | fletch transfer \
  --source-driver postgresql --source-uri "postgresql://..." \
  --dest-driver duckdb --dest-uri "output.duckdb" \
  --dest-table results \
  --query-file - \
  --yes
```

#### Dry Run

Validate all inputs without executing the transfer:

```bash
fletch transfer \
  --source-driver sqlite --source-uri "file:test.db" \
  --dest-driver duckdb --dest-uri ":memory:" \
  --dest-table export \
  --query "SELECT 1" \
  --dry-run
```

#### Interactive Mode

```bash
# Default: starts wizard when no flags provided
fletch transfer

# Explicit interactive mode
fletch transfer --interactive
```

The wizard guides you through:
1. Select source database type and enter connection URI
2. Select destination database type and enter connection URI
3. Enter destination table name
4. Select ingest mode (create/append/replace)
5. Select transfer mode (batch/streaming)
6. Enter SQL query
7. Review summary and confirm

---

### `test-connection` - Validate Database Connectivity

```bash
# Text output
fletch test-connection --driver postgresql --uri "postgresql://user:pass@localhost:5432/mydb"

# JSON output
fletch test-connection --driver duckdb --uri ":memory:" --output json
```

JSON output:
```json
{
  "status": "success",
  "driver": "duckdb",
  "message": "Connection successful"
}
```

---

### `list-drivers` - List Supported Drivers

```bash
# Text output
fletch list-drivers

# JSON output
fletch list-drivers --output json
```

---

### `version` - Print Version

```bash
fletch version
fletch version --output json
```

## Exit Codes

The CLI uses distinct exit codes for programmatic error handling:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments / usage error |
| 3 | Source connection failed |
| 4 | Destination connection failed |
| 5 | Query execution failed |
| 6 | Missing ADBC driver |
| 7 | Driver installation failed |

## Ingest Modes

- **Create**: Create a new table (fails if table already exists)
- **Append**: Add data to an existing table (table must exist)
- **Replace**: Drop and recreate the table with new data

> **Note**: The "create_append" mode (create if not exists, append if exists) is defined in the ADBC specification but is not supported by all drivers (e.g., DuckDB). For this workflow, use **Replace** for the first run, then **Append** for subsequent runs.

## Transfer Modes

- **Batch Mode**: Processes data batch-by-batch with transaction support
  - Best for: Datasets where you want fine-grained control and progress tracking
  - Advantages: Transaction safety, detailed batch-level progress

- **Streaming Mode**: Uses efficient BindStream for optimal performance
  - Best for: Large datasets that need maximum efficiency
  - Advantages: Most memory-efficient, lets ADBC handle batching internally

## Example Sessions

### PostgreSQL to DuckDB (Non-Interactive)

```bash
fletch transfer \
  --source-driver postgresql \
  --source-uri "postgresql://analytics:pass@db.example.com:5432/prod" \
  --dest-driver duckdb \
  --dest-uri "./warehouse.duckdb" \
  --dest-table orders_2025 \
  --ingest-mode create \
  --transfer-mode streaming \
  --query "SELECT * FROM orders WHERE year = 2025" \
  --yes --output json
```

### BigQuery to MotherDuck (Interactive)

```
$ fletch transfer

🚀 ADBC Data Transfer CLI
========================

📦 Select SOURCE Database
? Select SOURCE database type: BigQuery
  Example: bigquery://project-id/dataset?credentialsFile=/path/to/key.json
? BigQuery connection URI: ********

📦 Select DESTINATION Database
? Select DESTINATION database type: MotherDuck
  Example: md:database_name?motherduck_token=your_token
? MotherDuck connection URI: ********

🎯 Destination Table
? Enter destination table name: analytics_export

🔧 Table Ingest Mode
? Select ingest mode: Replace - Drop and recreate table

⚡ Select Transfer Mode
? Select transfer mode: Streaming - Process data in chunks

📝 SQL Query
? Enter SQL query to execute: SELECT * FROM `my-project.analytics.events` WHERE date >= '2025-01-01'

📋 Transfer Summary:
Source: BigQuery (bigquery)
Destination: MotherDuck (motherduck)
Target Table: analytics_export
Ingest Mode: adbc.ingest.mode.replace
Transfer Mode: streaming
Query: SELECT * FROM `my-project.analytics.events` WHERE date >= '2025-01-01'

? Proceed with transfer: y

🔄 Starting data transfer...
⚡ Processing in streaming mode...
  Binding stream to destination...
  Executing stream insert...
  Committing transaction...
✓ Total inserted: 125847 rows
✅ Transfer completed successfully!
```

## Supported Databases

The CLI supports any database with an ADBC driver and **automatically handles driver-specific configuration**:

- **PostgreSQL** (`driver: postgresql`) - Full support
- **SQLite** (`driver: sqlite`) - Full support
- **DuckDB** (`driver: duckdb`) - Full support
- **BigQuery** (`driver: bigquery`) - Full support
- **MotherDuck** (`driver: motherduck`) - Full support
- **Snowflake** (`driver: snowflake`) - Full support
- **Flight SQL** (`driver: flightsql`) - Full support
- **MySQL** - Experimental
- And more...

The CLI automatically maps connection parameters to the correct format for each driver (e.g., DuckDB uses `path` while SQLite uses `uri`).

Check the [ADBC documentation](https://arrow.apache.org/adbc/) for a complete list of drivers.

### Transaction Handling

The CLI attempts to use manual transaction control for data transfers. However, some databases (like DuckDB) operate in **autocommit mode** by default, where each statement is automatically committed.

If you see the message "Data auto-committed (manual transaction control not available)", this means:
- Your data transfer was successful
- The database automatically committed the data
- Manual transaction control isn't available for this database/driver combination

This is normal behavior and doesn't indicate an error.

## Configuration

### Authentication Setup

#### BigQuery Authentication

BigQuery requires Google Cloud authentication. Choose one method:

**Method 1: Service Account (Recommended for Production)**
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a service account with BigQuery permissions
3. Download the JSON key file
4. Use in connection string: `bigquery://project/dataset?credentialsFile=/path/to/key.json`

**Method 2: Application Default Credentials (ADC)**
1. Install Google Cloud CLI: `brew install google-cloud-sdk` (macOS)
2. Authenticate: `gcloud auth application-default login`
3. Use connection string: `bigquery://project-id/dataset`

**Method 3: Environment Variable**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

#### MotherDuck Authentication

MotherDuck requires an API token:

1. Sign up at [motherduck.com](https://motherduck.com)
2. Get your API token from the dashboard
3. Use one of these methods:

**Method 1: Connection String**
```
md:database_name?motherduck_token=your_token
```

**Method 2: Environment Variable (Recommended)**
```bash
export MOTHERDUCK_TOKEN=your_token
# Then use: md:database_name
```

**Method 3: Token File**
MotherDuck automatically checks `~/.motherduck/token` if it exists.

### Connection URI Format

Each database has its own URI format:

**PostgreSQL:**
```
postgresql://user:password@host:port/database?param=value
```

**SQLite:**
```
file:path/to/database.db
```

**DuckDB:**
```
path/to/database.duckdb
# Or for in-memory:
:memory:
```

**BigQuery:**
```
# Using service account JSON key file:
bigquery://project-id/dataset?credentialsFile=/path/to/service-account.json

# Using Application Default Credentials (ADC):
bigquery://project-id/dataset

# Make sure to set GOOGLE_APPLICATION_CREDENTIALS environment variable
# or authenticate via: gcloud auth application-default login
```

**MotherDuck:**
```
# With token in connection string:
md:database_name?motherduck_token=your_api_token

# Using environment variable (recommended for security):
md:database_name
# Set: export MOTHERDUCK_TOKEN=your_api_token

# Get your API token from: https://motherduck.com
```

## Development

### Project Structure

```
.
├── main.go           # Entry point, root command, version command
├── transfer.go       # Transfer subcommand (interactive + flag-based modes)
├── connection.go     # Database config, connection helpers, test-connection command
├── drivers.go        # Driver installation, list-drivers command
├── output.go         # Exit codes, result types, JSON output helpers
├── go.mod            # Go module definition
├── go.sum            # Dependency checksums
└── README.md         # This file
```

### Dependencies

- `github.com/apache/arrow-adbc/go/adbc` - ADBC client library
- `github.com/apache/arrow-go/v18` - Apache Arrow for Go
- `github.com/manifoldco/promptui` - Interactive CLI prompts
- `github.com/spf13/cobra` - CLI framework with subcommands and flag parsing

### Building

```bash
go build -o fletch
```

### Running Tests

```bash
go test ./...
```

## Future Enhancements

- [x] Add support for bulk inserts to destination
- [x] Transaction support with automatic rollback
- [x] Multiple ingest modes (create/append/replace)
- [x] Non-interactive CLI flags for AI agent / script use
- [x] JSON output mode for machine-readable results
- [x] Distinct exit codes for automated error handling
- [x] Dry-run mode to preview transfer without executing
- [x] Query from file / stdin support
- [x] `test-connection` subcommand
- [x] `list-drivers` subcommand
- [ ] Progress bar for large transfers
- [ ] Configuration file support for saving connection profiles
- [ ] Schema mapping and transformation
- [ ] Data type conversion and validation
- [ ] Parallel batch processing
- [ ] Resume capability for interrupted transfers
- [ ] Metrics and performance monitoring

## License

Copyright 2026 Columnar Technologies Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on the project repository.
