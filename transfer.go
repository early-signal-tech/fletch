// Copyright 2026 Columnar Technologies Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"
)

type TransferMode string

const (
	BatchMode     TransferMode = "batch"
	StreamingMode TransferMode = "streaming"
)

var transferCmd = &cobra.Command{
	Use:   "transfer",
	Short: "Transfer data between databases",
	Long: `Transfer data from a source database to a destination database using ADBC.

When run without flags, starts an interactive wizard. When all required flags
are provided, runs non-interactively (suitable for scripting and AI agents).

Required flags for non-interactive mode:
  --source-driver, --source-uri, --dest-driver, --dest-uri, --dest-table,
  --query (or --query-file)

Examples:
  # Interactive mode
  fletch transfer

  # Non-interactive mode
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

  # With JSON output for programmatic consumption
  fletch transfer \
    --source-driver sqlite --source-uri "file:data.db" \
    --dest-driver duckdb --dest-uri ":memory:" \
    --dest-table export --query "SELECT * FROM users" \
    --yes --output json

  # Read query from file
  fletch transfer \
    --source-driver postgresql --source-uri "postgresql://..." \
    --dest-driver duckdb --dest-uri "output.duckdb" \
    --dest-table results --query-file query.sql --yes`,
	RunE: runTransfer,
}

func init() {
	f := transferCmd.Flags()
	f.String("source-driver", "", "Source database driver (e.g., postgresql, sqlite, duckdb)")
	f.String("source-uri", "", "Source database connection URI")
	f.String("dest-driver", "", "Destination database driver")
	f.String("dest-uri", "", "Destination database connection URI")
	f.String("dest-table", "", "Destination table name")
	f.String("ingest-mode", "create", "Table ingest mode: create, append, replace")
	f.String("transfer-mode", "batch", "Transfer mode: batch, streaming")
	f.String("query", "", "SQL query to execute on source")
	f.String("query-file", "", "Path to file containing SQL query (use - for stdin)")
	f.BoolP("yes", "y", false, "Skip confirmation prompt")
	f.Bool("dry-run", false, "Validate inputs and show transfer plan without executing")
	f.Bool("interactive", false, "Force interactive mode")
	f.Bool("auto-install-drivers", false, "Automatically install missing drivers")
	f.Bool("no-install-drivers", false, "Fail immediately if drivers are missing")
}

func shouldRunInteractive(cmd *cobra.Command) bool {
	interactive, _ := cmd.Flags().GetBool("interactive")
	if interactive {
		return true
	}

	transferFlags := []string{
		"source-driver", "source-uri", "dest-driver", "dest-uri",
		"dest-table", "query", "query-file",
	}
	for _, name := range transferFlags {
		if cmd.Flags().Changed(name) {
			return false
		}
	}
	return true
}

func runTransfer(cmd *cobra.Command, args []string) error {
	if shouldRunInteractive(cmd) {
		return runInteractiveTransfer(cmd)
	}
	return runFlagBasedTransfer(cmd)
}

// runInteractiveTransfer preserves the original interactive wizard experience.
func runInteractiveTransfer(cmd *cobra.Command) error {
	fmt.Println("🚀 ADBC Data Transfer CLI")
	fmt.Println("========================")
	fmt.Println()

	sourceConfig, err := selectDatabase("SOURCE")
	if err != nil {
		exitWithError(fmt.Sprintf("error selecting source: %v", err), ExitUsageError)
	}

	destConfig, err := selectDatabase("DESTINATION")
	if err != nil {
		exitWithError(fmt.Sprintf("error selecting destination: %v", err), ExitUsageError)
	}

	destTable, err := getDestinationTable()
	if err != nil {
		exitWithError(fmt.Sprintf("error getting destination table: %v", err), ExitUsageError)
	}

	ingestMode, err := selectIngestMode()
	if err != nil {
		exitWithError(fmt.Sprintf("error selecting ingest mode: %v", err), ExitUsageError)
	}

	mode, err := selectTransferMode()
	if err != nil {
		exitWithError(fmt.Sprintf("error selecting transfer mode: %v", err), ExitUsageError)
	}

	query, err := getQuery()
	if err != nil {
		exitWithError(fmt.Sprintf("error getting query: %v", err), ExitUsageError)
	}

	fmt.Println("\n📋 Transfer Summary:")
	fmt.Printf("Source: %s (%s)\n", sourceConfig.Name, sourceConfig.Driver)
	fmt.Printf("Destination: %s (%s)\n", destConfig.Name, destConfig.Driver)
	fmt.Printf("Target Table: %s\n", destTable)
	fmt.Printf("Ingest Mode: %s\n", ingestMode)
	fmt.Printf("Transfer Mode: %s\n", mode)
	fmt.Printf("Query: %s\n\n", query)

	skipConfirm, _ := cmd.Flags().GetBool("yes")
	if !skipConfirm && !confirmExecution() {
		fmt.Println("Transfer cancelled.")
		os.Exit(0)
	}

	fmt.Println("\n🔄 Starting data transfer...")
	startTime := time.Now()
	result, err := executeTransfer(sourceConfig, destConfig, query, destTable, ingestMode, mode)
	duration := time.Since(startTime)

	if err != nil {
		isMissing, driverName := isMissingDriverError(err)
		if isMissing {
			autoInstall, _ := cmd.Flags().GetBool("auto-install-drivers")
			noInstall, _ := cmd.Flags().GetBool("no-install-drivers")

			if installErr := handleMissingDriver(driverName, autoInstall, noInstall, true); installErr != nil {
				exitWithError(installErr.Error(), ExitMissingDriver)
			}

			fmt.Println("\n🔄 Retrying data transfer with newly installed drivers...")
			startTime = time.Now()
			result, err = executeTransfer(sourceConfig, destConfig, query, destTable, ingestMode, mode)
			duration = time.Since(startTime)
			if err != nil {
				exitWithError(fmt.Sprintf("transfer failed after driver installation: %v", err), ExitGeneralError)
			}
		} else {
			exitWithError(fmt.Sprintf("transfer failed: %v", err), ExitGeneralError)
		}
	}

	if isJSON() {
		outputJSON(TransferResult{
			Status:          "success",
			RowsTransferred: result.rows,
			Batches:         result.batches,
			Source:          DBInfo{Driver: sourceConfig.Driver, Name: sourceConfig.Name},
			Destination:     DBInfo{Driver: destConfig.Driver, Name: destConfig.Name},
			Table:           destTable,
			IngestMode:      ingestMode,
			TransferMode:    string(mode),
			Query:           query,
			DurationMs:      duration.Milliseconds(),
		})
	} else {
		fmt.Println("✅ Transfer completed successfully!")
	}
	return nil
}

// runFlagBasedTransfer runs the transfer using CLI flags (non-interactive, agent-friendly).
func runFlagBasedTransfer(cmd *cobra.Command) error {
	sourceDriver, _ := cmd.Flags().GetString("source-driver")
	sourceURI, _ := cmd.Flags().GetString("source-uri")
	destDriver, _ := cmd.Flags().GetString("dest-driver")
	destURI, _ := cmd.Flags().GetString("dest-uri")
	destTable, _ := cmd.Flags().GetString("dest-table")
	ingestModeStr, _ := cmd.Flags().GetString("ingest-mode")
	transferModeStr, _ := cmd.Flags().GetString("transfer-mode")
	skipConfirm, _ := cmd.Flags().GetBool("yes")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	autoInstall, _ := cmd.Flags().GetBool("auto-install-drivers")
	noInstall, _ := cmd.Flags().GetBool("no-install-drivers")

	var missing []string
	if sourceDriver == "" {
		missing = append(missing, "--source-driver")
	}
	if sourceURI == "" {
		missing = append(missing, "--source-uri")
	}
	if destDriver == "" {
		missing = append(missing, "--dest-driver")
	}
	if destURI == "" {
		missing = append(missing, "--dest-uri")
	}
	if destTable == "" {
		missing = append(missing, "--dest-table")
	}

	query, err := resolveQueryFlag(cmd)
	if err != nil {
		missing = append(missing, "--query or --query-file")
	}

	if len(missing) > 0 {
		exitWithError(
			fmt.Sprintf("missing required flags: %s\n\nProvide all required flags for non-interactive mode, or use --interactive for the wizard.",
				strings.Join(missing, ", ")),
			ExitUsageError,
		)
	}

	ingestMode, err := resolveIngestMode(ingestModeStr)
	if err != nil {
		exitWithError(err.Error(), ExitUsageError)
	}

	var transferMode TransferMode
	switch strings.ToLower(transferModeStr) {
	case "batch":
		transferMode = BatchMode
	case "streaming":
		transferMode = StreamingMode
	default:
		exitWithError(fmt.Sprintf("invalid transfer mode: %s (valid: batch, streaming)", transferModeStr), ExitUsageError)
	}

	sourceConfig := &DatabaseConfig{Driver: sourceDriver, URI: sourceURI, Name: sourceDriver}
	destConfig := &DatabaseConfig{Driver: destDriver, URI: destURI, Name: destDriver}

	if dryRun {
		return outputDryRun(sourceConfig, destConfig, destTable, ingestMode, transferMode, query)
	}

	if !skipConfirm && !isJSON() && !isQuiet() {
		logInfo("\n📋 Transfer Summary:\n")
		logInfo("Source: %s (%s)\n", sourceConfig.Name, sourceConfig.Driver)
		logInfo("Destination: %s (%s)\n", destConfig.Name, destConfig.Driver)
		logInfo("Target Table: %s\n", destTable)
		logInfo("Ingest Mode: %s\n", ingestMode)
		logInfo("Transfer Mode: %s\n", transferMode)
		logInfo("Query: %s\n\n", query)

		if !confirmExecution() {
			fmt.Println("Transfer cancelled.")
			os.Exit(0)
		}
	}

	logInfoln("\n🔄 Starting data transfer...")
	startTime := time.Now()
	result, err := executeTransfer(sourceConfig, destConfig, query, destTable, ingestMode, transferMode)
	duration := time.Since(startTime)

	if err != nil {
		isMissing, driverName := isMissingDriverError(err)
		if isMissing {
			if installErr := handleMissingDriver(driverName, autoInstall, noInstall, false); installErr != nil {
				exitWithError(installErr.Error(), ExitMissingDriver)
			}

			logInfoln("\n🔄 Retrying data transfer with newly installed drivers...")
			startTime = time.Now()
			result, err = executeTransfer(sourceConfig, destConfig, query, destTable, ingestMode, transferMode)
			duration = time.Since(startTime)
			if err != nil {
				exitWithError(fmt.Sprintf("transfer failed after driver installation: %v", err), ExitGeneralError)
			}
		} else {
			code := classifyTransferError(err)
			exitWithError(fmt.Sprintf("transfer failed: %v", err), code)
		}
	}

	if isJSON() {
		outputJSON(TransferResult{
			Status:          "success",
			RowsTransferred: result.rows,
			Batches:         result.batches,
			Source:          DBInfo{Driver: sourceConfig.Driver, Name: sourceConfig.Name},
			Destination:     DBInfo{Driver: destConfig.Driver, Name: destConfig.Name},
			Table:           destTable,
			IngestMode:      ingestMode,
			TransferMode:    string(transferMode),
			Query:           query,
			DurationMs:      duration.Milliseconds(),
		})
	} else {
		logInfoln("✅ Transfer completed successfully!")
	}
	return nil
}

func resolveQueryFlag(cmd *cobra.Command) (string, error) {
	query, _ := cmd.Flags().GetString("query")
	queryFile, _ := cmd.Flags().GetString("query-file")

	if query != "" && queryFile != "" {
		return "", fmt.Errorf("cannot specify both --query and --query-file")
	}

	if queryFile != "" {
		if queryFile == "-" {
			data, err := io.ReadAll(os.Stdin)
			if err != nil {
				return "", fmt.Errorf("failed to read query from stdin: %w", err)
			}
			return strings.TrimSpace(string(data)), nil
		}
		data, err := os.ReadFile(queryFile)
		if err != nil {
			return "", fmt.Errorf("failed to read query file %s: %w", queryFile, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	if query != "" {
		return query, nil
	}

	return "", fmt.Errorf("no query specified")
}

func outputDryRun(source, dest *DatabaseConfig, destTable, ingestMode string, transferMode TransferMode, query string) error {
	if isJSON() {
		outputJSON(map[string]interface{}{
			"status":        "dry_run",
			"source":        DBInfo{Driver: source.Driver, Name: source.Name},
			"destination":   DBInfo{Driver: dest.Driver, Name: dest.Name},
			"table":         destTable,
			"ingest_mode":   ingestMode,
			"transfer_mode": string(transferMode),
			"query":         query,
			"validated":     true,
		})
	} else {
		fmt.Println("📋 Dry Run - Transfer Plan:")
		fmt.Println("===========================")
		fmt.Printf("Source:        %s (driver: %s)\n", source.Name, source.Driver)
		fmt.Printf("               URI provided: yes\n")
		fmt.Printf("Destination:   %s (driver: %s)\n", dest.Name, dest.Driver)
		fmt.Printf("               URI provided: yes\n")
		fmt.Printf("Target Table:  %s\n", destTable)
		fmt.Printf("Ingest Mode:   %s\n", ingestMode)
		fmt.Printf("Transfer Mode: %s\n", transferMode)
		fmt.Printf("Query:         %s\n", query)
		fmt.Println()
		fmt.Println("✓ All inputs validated. Remove --dry-run to execute the transfer.")
	}
	return nil
}

func classifyTransferError(err error) int {
	msg := err.Error()
	if strings.Contains(msg, "source") && strings.Contains(msg, "connection") {
		return ExitSourceConnError
	}
	if strings.Contains(msg, "destination") && strings.Contains(msg, "connection") {
		return ExitDestConnError
	}
	if strings.Contains(msg, "query") || strings.Contains(msg, "execute") {
		return ExitQueryError
	}
	return ExitGeneralError
}

type transferResult struct {
	rows    int64
	batches int
}

func executeTransfer(source, dest *DatabaseConfig, query, destTable, ingestMode string, mode TransferMode) (*transferResult, error) {
	ctx := context.Background()

	var srcDriver drivermgr.Driver
	srcConfig := buildDriverConfig(source)
	srcDB, err := srcDriver.NewDatabase(srcConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create source database: %w", err)
	}
	defer srcDB.Close()

	srcConn, err := srcDB.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open source connection: %w", err)
	}
	defer srcConn.Close()

	var destDrv drivermgr.Driver
	destCfg := buildDriverConfig(dest)
	destDB, err := destDrv.NewDatabase(destCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination database: %w", err)
	}
	defer destDB.Close()

	destConn, err := destDB.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open destination connection: %w", err)
	}
	defer destConn.Close()

	stmt, err := srcConn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, fmt.Errorf("failed to set query: %w", err)
	}

	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer stream.Release()

	switch mode {
	case BatchMode:
		return transferBatch(ctx, stream, destConn, destTable, ingestMode)
	case StreamingMode:
		return transferStreaming(ctx, stream, destConn, destTable, ingestMode)
	default:
		return nil, fmt.Errorf("unknown transfer mode: %s", mode)
	}
}

func transferBatch(ctx context.Context, stream array.RecordReader, destConn adbc.Connection, destTable, ingestMode string) (*transferResult, error) {
	logInfoln("📦 Processing in batch mode...")

	stmt, err := destConn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create destination statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, destTable); err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("failed to set target table: %w", err)
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestMode, ingestMode); err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("failed to set ingest mode: %w", err)
	}

	batchCount := 0
	recordCount := int64(0)

	for stream.Next() {
		batch := stream.Record()
		batchCount++
		recordCount += batch.NumRows()

		logInfo("  Processing batch %d: %d rows...", batchCount, batch.NumRows())

		if err := stmt.Bind(ctx, batch); err != nil {
			batch.Release()
			destConn.Rollback(ctx)
			return nil, fmt.Errorf("failed to bind batch %d: %w", batchCount, err)
		}

		rowsAffected, err := stmt.ExecuteUpdate(ctx)
		if err != nil {
			batch.Release()
			destConn.Rollback(ctx)
			return nil, fmt.Errorf("failed to insert batch %d: %w", batchCount, err)
		}

		logInfo(" ✓ Inserted %d rows\n", rowsAffected)
		batch.Release()

		// After the first batch creates the table, switch to append for subsequent batches
		if batchCount == 1 && ingestMode == adbc.OptionValueIngestModeCreate {
			if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
				destConn.Rollback(ctx)
				return nil, fmt.Errorf("failed to switch to append mode after table creation: %w", err)
			}
		}
	}

	if err := stream.Err(); err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("error reading stream: %w", err)
	}

	logInfoln("  Committing transaction...")
	if err := destConn.Commit(ctx); err != nil {
		logInfoln("  ℹ️  Data auto-committed (manual transaction control not available)")
	}

	logInfo("✓ Total inserted: %d batches, %d rows\n", batchCount, recordCount)
	return &transferResult{rows: recordCount, batches: batchCount}, nil
}

func transferStreaming(ctx context.Context, stream array.RecordReader, destConn adbc.Connection, destTable, ingestMode string) (*transferResult, error) {
	logInfoln("⚡ Processing in streaming mode...")

	stmt, err := destConn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create destination statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, destTable); err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("failed to set target table: %w", err)
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestMode, ingestMode); err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("failed to set ingest mode: %w", err)
	}

	logInfoln("  Binding stream to destination...")

	if err := stmt.BindStream(ctx, stream); err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("failed to bind stream: %w", err)
	}

	logInfoln("  Executing stream insert...")

	rowsAffected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		destConn.Rollback(ctx)
		return nil, fmt.Errorf("failed to execute stream insert: %w", err)
	}

	logInfoln("  Committing transaction...")
	if err := destConn.Commit(ctx); err != nil {
		logInfoln("  ℹ️  Data auto-committed (manual transaction control not available)")
	}

	logInfo("✓ Total inserted: %d rows\n", rowsAffected)
	return &transferResult{rows: rowsAffected, batches: 0}, nil
}
