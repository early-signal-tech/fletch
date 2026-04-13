# Agent Instructions for Fletch

This file provides instructions for AI agents (Cursor, Claude Code, GitHub Copilot, etc.) working with or invoking the `fletch` tool.

## What is Fletch?

A CLI for transferring data between databases using Apache Arrow Database Connectivity (ADBC). It supports PostgreSQL, SQLite, DuckDB, BigQuery, MotherDuck, Snowflake, Flight SQL, and local Parquet files (as a destination).

## Building

```bash
cd /path/to/go-test
go build -o fletch
```

Requires Go 1.25+.

## Non-Interactive Usage (Agent Mode)

**Always use non-interactive mode.** Provide all required flags and `--yes` to skip prompts. Add `--output json` for machine-readable output.

### Required Flags

| Flag | Description |
|------|-------------|
| `--source-driver` | Source database driver name |
| `--source-uri` | Source connection URI |
| `--dest-driver` | Destination database driver name |
| `--dest-uri` | Destination connection URI |
| `--dest-table` | Destination table name (not required when `--dest-driver parquet`) |
| `--query` or `--query-file` | SQL query (use `--query-file -` for stdin) |

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--ingest-mode` | `create` | `create`, `append`, or `replace` |
| `--transfer-mode` | `batch` | `batch` or `streaming` |
| `--yes` / `-y` | `false` | Skip confirmation (required for non-interactive) |
| `--dry-run` | `false` | Validate without executing |
| `--output` / `-o` | `text` | `text`, `json`, or `quiet` |
| `--auto-install-drivers` | `false` | Auto-install missing ADBC drivers |
| `--no-install-drivers` | `false` | Fail immediately if drivers missing |

### Canonical Invocation Pattern

```bash
fletch transfer \
  --source-driver <driver> \
  --source-uri "<uri>" \
  --dest-driver <driver> \
  --dest-uri "<uri>" \
  --dest-table <table_name> \
  --query "<SQL>" \
  --ingest-mode <create|append|replace> \
  --transfer-mode <batch|streaming> \
  --yes \
  --output json
```

## Connection URI Formats

| Driver | Flag Value | URI Format |
|--------|-----------|------------|
| PostgreSQL | `postgresql` | `postgresql://user:pass@host:port/database` |
| SQLite | `sqlite` | `file:path/to/db.sqlite` |
| DuckDB | `duckdb` | `path/to/db.duckdb` or `:memory:` |
| BigQuery | `bigquery` | `bigquery://project-id/dataset` |
| MotherDuck | `motherduck` | `md:database_name?motherduck_token=TOKEN` or `md:database_name` (with `MOTHERDUCK_TOKEN` env var) |
| Snowflake | `snowflake` | `snowflake://user:pass@account/database` |
| Flight SQL | `flightsql` | `grpc://host:port` |
| Parquet File | `parquet` | `path/to/output.parquet` (destination only; built-in, no external driver needed) |
| Amazon S3 | `s3` | `s3://bucket/path/to/output.parquet` (destination only; writes Parquet; uses AWS credential chain) |

## BigQuery Authentication & Connection

**Important**: The ADBC BigQuery driver uses standard ADBC configuration parameters, not URI query parameters.

### Connection Format

```bash
fletch transfer \
  --source-driver bigquery \
  --source-uri "bigquery://project-id/dataset" \
  --dest-driver duckdb \
  --dest-uri "output.duckdb" \
  --dest-table my_table \
  --query "SELECT * FROM my_table" \
  --yes --output json
```

### Authentication Methods

1. **Application Default Credentials (ADC)** - Recommended for development
   ```bash
   gcloud auth application-default login
   # Creates: ~/.config/gcloud/application_default_credentials.json
   # Then use fletch with just: bigquery://project-id/dataset
   ```

2. **Environment Variable**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
   ```

3. **Service Account Key File** - Use with GOOGLE_APPLICATION_CREDENTIALS env var

### Reference Implementation

See [columnar-tech/adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts/tree/main/go/bigquery) for the official ADBC BigQuery Go example using driver configuration:

```go
db, err := drv.NewDatabase(map[string]string{
    "driver":                       "bigquery",
    "adbc.bigquery.sql.project_id": "my-gcp-project",
    "adbc.bigquery.sql.dataset_id": "bigquery-public-data",
})
```

## Amazon S3 Authentication & Credentials

**Important**: S3 destinations use the standard AWS credential chain. Credentials are NOT passed in the URI.

### Authentication Methods (in order of precedence)

1. **Environment Variables** - Recommended for scripts and CI/CD
   ```bash
   export AWS_ACCESS_KEY_ID="your-access-key"
   export AWS_SECRET_ACCESS_KEY="your-secret-key"
   export AWS_REGION="us-east-1"  # optional, defaults to us-east-1
   ```

2. **AWS Credentials File** - Recommended for local development
   ```bash
   # Create or edit ~/.aws/credentials
   [default]
   aws_access_key_id = your-access-key
   aws_secret_access_key = your-secret-key
   
   # Optional: ~/.aws/config for region
   [default]
   region = us-east-1
   ```

3. **IAM Role** - Recommended for AWS EC2/Lambda/ECS
   - Attach an IAM role with S3 permissions to your EC2 instance, Lambda function, or ECS task
   - No credentials required; AWS SDK automatically uses the role

4. **Temporary Credentials** - For temporary access
   ```bash
   export AWS_ACCESS_KEY_ID="temporary-key"
   export AWS_SECRET_ACCESS_KEY="temporary-secret"
   export AWS_SESSION_TOKEN="session-token"
   ```

### Loading from .env File

If you store credentials in a `.env` file:

```bash
# .env format
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1

# Load into shell session
set -a
source .env
set +a

# Then run fletch
fletch transfer --source-driver ... --dest-driver s3 ...
```

## MotherDuck Authentication & Token

MotherDuck supports two authentication methods:

### 1. Environment Variable (Recommended for automation)
```bash
export MOTHERDUCK_TOKEN="your-token"
fletch transfer \
  --source-driver motherduck \
  --source-uri "md:database_name" \
  --dest-driver duckdb \
  --dest-uri "output.duckdb" \
  --dest-table my_table \
  --query "SELECT * FROM table" \
  --yes --output json
```

### 2. Browser-based SSO (Recommended for development)
If no token is set, fletch will prompt for browser-based login:
```bash
fletch transfer \
  --source-driver motherduck \
  --source-uri "md:database_name" \
  --dest-driver duckdb \
  --dest-uri "output.duckdb" \
  --dest-table my_table \
  --query "SELECT * FROM table" \
  --yes --output json
# Opens browser for SSO login automatically
```

## PostgreSQL & Snowflake Credentials

### PostgreSQL
Include credentials in the URI:
```bash
fletch transfer \
  --source-driver postgresql \
  --source-uri "postgresql://username:password@host:5432/database" \
  ...
```

**Security Note**: Passwords in URIs are visible in process listings. For production, use environment variables or connection poolers.

### Snowflake
Include credentials in the URI:
```bash
fletch transfer \
  --source-driver snowflake \
  --source-uri "snowflake://username:password@account/database/schema" \
  ...
```

## JSON Output

### Success

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

### Error

```json
{
  "error": "missing required flags: --source-uri, --dest-driver",
  "error_code": 2
}
```

### Dry Run

```json
{
  "status": "dry_run",
  "source": { "driver": "sqlite", "name": "sqlite" },
  "destination": { "driver": "duckdb", "name": "duckdb" },
  "table": "export",
  "ingest_mode": "adbc.ingest.mode.create",
  "transfer_mode": "batch",
  "query": "SELECT 1",
  "validated": true
}
```

## Exit Codes

| Code | Meaning | Suggested Action |
|------|---------|------------------|
| 0 | Success | None |
| 1 | General error | Inspect error message |
| 2 | Invalid arguments | Fix flags and retry |
| 3 | Source connection failed | Verify source URI and credentials |
| 4 | Destination connection failed | Verify destination URI and credentials |
| 5 | Query execution failed | Fix SQL query |
| 6 | Missing ADBC driver | Retry with `--auto-install-drivers` |
| 7 | Driver installation failed | Install driver manually |

## Other Subcommands

### Test a connection

```bash
fletch test-connection --driver <driver> --uri "<uri>" --output json
```

### List available drivers

```bash
fletch list-drivers --output json
```

### Print version

```bash
fletch version --output json
```

## Decision Guide

### Ingest Mode

- **`create`**: First-time load into a new table. Fails if the table already exists.
- **`append`**: Add rows to an existing table. Table must already exist with a compatible schema.
- **`replace`**: Drop and recreate. Use for full refreshes or idempotent pipelines.

### Transfer Mode

- **`batch`**: Processes records batch-by-batch with transaction safety. Better for smaller datasets or when you need progress visibility.
- **`streaming`**: Uses ADBC `BindStream` for maximum throughput. Better for large datasets.

### Driver Installation

When running in automated pipelines, use `--auto-install-drivers` to allow the CLI to install missing drivers without prompts. Use `--no-install-drivers` to fail fast if drivers are missing (e.g., in CI where you control the environment).

## Examples

### SQLite to DuckDB

```bash
fletch transfer \
  --source-driver sqlite --source-uri "file:data.db" \
  --dest-driver duckdb --dest-uri "warehouse.duckdb" \
  --dest-table users_export \
  --query "SELECT * FROM users WHERE active = 1" \
  --ingest-mode create --transfer-mode batch \
  --yes --output json
```

### PostgreSQL to MotherDuck (streaming)

```bash
export MOTHERDUCK_TOKEN=your_token
fletch transfer \
  --source-driver postgresql \
  --source-uri "postgresql://analytics:pass@db.example.com:5432/prod" \
  --dest-driver motherduck --dest-uri "md:analytics" \
  --dest-table orders_2025 \
  --query "SELECT * FROM orders WHERE created_at >= '2025-01-01'" \
  --ingest-mode replace --transfer-mode streaming \
  --yes --output json --auto-install-drivers
```

### BigQuery to DuckDB

```bash
fletch transfer \
  --source-driver bigquery \
  --source-uri "bigquery://cloud-analytics-457323/cloud-analytics-457323" \
  --dest-driver duckdb --dest-uri "analytics_export.duckdb" \
  --dest-table streaming_data \
  --query "SELECT * FROM streaming_data WHERE date >= '2025-01-01'" \
  --ingest-mode create --transfer-mode streaming \
  --yes --output json
```

### DuckDB to BigQuery

```bash
fletch transfer \
  --source-driver duckdb --source-uri "local_data.duckdb" \
  --dest-driver bigquery \
  --dest-uri "bigquery://my-project/my-dataset" \
  --dest-table imported_data \
  --query "SELECT * FROM source_table" \
  --ingest-mode create --transfer-mode streaming \
  --yes --output json
```

### Query from file via stdin

```bash
cat complex_query.sql | fletch transfer \
  --source-driver postgresql --source-uri "postgresql://..." \
  --dest-driver duckdb --dest-uri "output.duckdb" \
  --dest-table results --query-file - \
  --yes --output json
```

### PostgreSQL to Amazon S3 (Parquet)

```bash
fletch transfer \
 --source-driver postgresql \
 --source-uri "postgresql://user:pass@host:5432/mydb" \
 --dest-driver s3 \
 --dest-uri "s3://my-bucket/exports/orders_2025.parquet" \
 --query "SELECT * FROM orders WHERE year = 2025" \
 --ingest-mode create \
 --yes --output json
```

Note: `--dest-table` is not required. Auth uses the standard AWS credential chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars, `~/.aws/credentials`, or IAM roles). Use `--ingest-mode replace` to overwrite an existing object. Append mode is not supported.

### PostgreSQL to Parquet file

```bash
fletch transfer \
  --source-driver postgresql \
  --source-uri "postgresql://user:pass@host:5432/mydb" \
  --dest-driver parquet \
  --dest-uri "orders_export.parquet" \
  --query "SELECT * FROM orders WHERE year = 2025" \
  --ingest-mode create \
  --yes --output json
```

Note: `--dest-table` is not required when `--dest-driver parquet`. Use `--ingest-mode replace` to overwrite an existing file. Snappy compression is applied automatically. Append mode is not supported for Parquet.

### Validate before executing

```bash
fletch transfer \
  --source-driver sqlite --source-uri "file:test.db" \
  --dest-driver duckdb --dest-uri ":memory:" \
  --dest-table export --query "SELECT 1" \
  --dry-run --output json
```

## Project Structure

| File | Purpose |
|------|---------|
| `main.go` | Entry point, root command, version command |
| `transfer.go` | Transfer subcommand (interactive + flag-based modes) |
| `connection.go` | Database config, connection helpers, test-connection command |
| `drivers.go` | Driver installation, list-drivers command |
| `output.go` | Exit codes, result types, JSON output helpers |
| `s3.go` | Amazon S3 destination writer (`isS3Driver`, `writeS3Dest`; streams data as Parquet to S3) |

- Uses `cobra` for CLI framework and `promptui` for interactive prompts.
- Driver configuration is mapped per-database in `buildDriverConfig()` in `connection.go`. DuckDB uses `path`, most others use `uri`.
- **BigQuery Configuration**: BigQuery connections parse the URI format `bigquery://project-id/dataset` and convert it to ADBC driver parameters:
  - `adbc.bigquery.sql.project_id` (extracted from project-id)
  - `adbc.bigquery.sql.dataset_id` (extracted from dataset)
  - This configuration approach is required by the ADBC BigQuery driver (see [adbc-quickstarts reference](https://github.com/columnar-tech/adbc-quickstarts/tree/main/go/bigquery))
- MotherDuck connections use the `duckdb` driver internally with an `md:` URI prefix.
- Transaction commit may silently succeed even when manual transaction control isn't supported (e.g., DuckDB autocommit). This is not an error.
- The `create` ingest mode automatically switches to `append` after the first batch to avoid "table already exists" errors on multi-batch transfers.
