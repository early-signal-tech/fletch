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
	"os"
	"runtime"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

type DatabaseConfig struct {
	Driver string
	URI    string
	Name   string
}

var testConnectionCmd = &cobra.Command{
	Use:   "test-connection",
	Short: "Test a database connection",
	Long:  "Validate connectivity to a database using ADBC.",
	RunE: func(cmd *cobra.Command, args []string) error {
		driver, _ := cmd.Flags().GetString("driver")
		uri, _ := cmd.Flags().GetString("uri")

		if driver == "" || uri == "" {
			return fmt.Errorf("--driver and --uri are required")
		}

		config := &DatabaseConfig{Driver: driver, URI: uri, Name: driver}
		err := testConnection(config)

		if isJSON() {
			result := ConnectionTestResult{Driver: driver}
			if err != nil {
				result.Status = "error"
				result.Error = err.Error()
				outputJSON(result)
				os.Exit(ExitSourceConnError)
			}
			result.Status = "success"
			result.Message = "Connection successful"
			outputJSON(result)
		} else {
			if err != nil {
				exitWithError(fmt.Sprintf("connection failed: %v", err), ExitSourceConnError)
			}
			fmt.Println("Connection successful!")
		}
		return nil
	},
}

func init() {
	testConnectionCmd.Flags().String("driver", "", "Database driver (e.g., postgresql, sqlite, duckdb)")
	testConnectionCmd.Flags().String("uri", "", "Connection URI")
}

func testConnection(config *DatabaseConfig) error {
	ctx := context.Background()
	var driver drivermgr.Driver
	driverConfig := buildDriverConfig(config)
	db, err := driver.NewDatabase(driverConfig)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	defer conn.Close()
	return nil
}

func buildDriverConfig(config *DatabaseConfig) map[string]string {
	driverConfig := map[string]string{
		"driver": config.Driver,
	}

	switch strings.ToLower(config.Driver) {
	case "duckdb":
		driverConfig["path"] = config.URI
	case "motherduck", "md":
		driverConfig["driver"] = "duckdb"
		driverConfig["path"] = config.URI
	case "sqlite", "sqlite3":
		driverConfig["uri"] = config.URI
	case "postgresql", "postgres":
		driverConfig["uri"] = config.URI
	case "bigquery":
		// Parse BigQuery URI format: bigquery://project-id/dataset
		// Extract project-id and dataset from URI
		parts := strings.TrimPrefix(config.URI, "bigquery://")
		pathParts := strings.Split(parts, "/")
		if len(pathParts) >= 2 {
			projectID := pathParts[0]
			dataset := pathParts[1]
			driverConfig["adbc.bigquery.sql.project_id"] = projectID
			driverConfig["adbc.bigquery.sql.dataset_id"] = dataset
		} else if len(pathParts) == 1 {
			// Allow just project-id if dataset is not provided
			driverConfig["adbc.bigquery.sql.project_id"] = pathParts[0]
		}
	case "snowflake":
		driverConfig["uri"] = config.URI
	case "flightsql":
		driverConfig["uri"] = config.URI
	case "parquet":
		driverConfig["path"] = config.URI
	default:
		driverConfig["uri"] = config.URI
	}

	return driverConfig
}

// needsURIMask returns true for drivers whose URIs contain embedded credentials.
func needsURIMask(driver string) bool {
	switch strings.ToLower(driver) {
	case "postgresql", "postgres", "snowflake":
		return true
	default:
		return false
	}
}

func getConnectionHelp(driver string) string {
	switch strings.ToLower(driver) {
	case "postgresql", "postgres":
		return "  Example: postgresql://user:password@host:port/database"
	case "sqlite", "sqlite3":
		return "  Example: file:path/to/database.db"
	case "duckdb":
		return "  Example: path/to/database.duckdb or :memory: for in-memory"
	case "bigquery":
		var envCmd string
		if runtime.GOOS == "windows" {
			envCmd = "set GOOGLE_APPLICATION_CREDENTIALS=path\\to\\key.json"
		} else {
			envCmd = "export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json"
		}
		return "  Example: bigquery://project-id/dataset?credentialsFile=/path/to/key.json\n" +
			"  Or use Application Default Credentials: bigquery://project-id/dataset\n" +
			"  To set credentials via environment: " + envCmd
	case "motherduck", "md":
		return "  Example: md:database_name\n" +
			"  Authentication is handled automatically via browser SSO."
	case "parquet":
		return "  Example: path/to/output.parquet or /absolute/path/data.parquet\n" +
			"  Built-in driver — no external installation required.\n" +
			"  Ingest modes: create (fail if exists), replace (overwrite). Append is not supported."
	default:
		return ""
	}
}

func resolveIngestMode(mode string) (string, error) {
	switch strings.ToLower(mode) {
	case "create":
		return adbc.OptionValueIngestModeCreate, nil
	case "append":
		return adbc.OptionValueIngestModeAppend, nil
	case "replace":
		return adbc.OptionValueIngestModeReplace, nil
	default:
		return "", fmt.Errorf("invalid ingest mode: %s (valid: create, append, replace)", mode)
	}
}

// selectDatabase prompts the user to choose a database type and enter connection details.
// Parquet is only available as a destination, not a source.
func selectDatabase(dbType string) (*DatabaseConfig, error) {
	fmt.Printf("\n📦 Select %s Database\n", dbType)

	isDestination := dbType == "DESTINATION"

	allTemplates := []DatabaseConfig{
		{Name: "PostgreSQL", Driver: "postgresql", URI: ""},
		{Name: "SQLite", Driver: "sqlite", URI: ""},
		{Name: "DuckDB", Driver: "duckdb", URI: ""},
		{Name: "BigQuery", Driver: "bigquery", URI: ""},
		{Name: "MotherDuck", Driver: "motherduck", URI: ""},
		{Name: "Parquet File", Driver: "parquet", URI: ""},
		{Name: "Custom", Driver: "", URI: ""},
	}

	var templates []DatabaseConfig
	for _, t := range allTemplates {
		if t.Driver == "parquet" && !isDestination {
			continue
		}
		templates = append(templates, t)
	}

	items := make([]string, len(templates))
	for i, t := range templates {
		items[i] = t.Name
	}

	promptDB := promptui.Select{
		Label: fmt.Sprintf("Select %s database type", dbType),
		Items: items,
	}

	idx, _, err := promptDB.Run()
	if err != nil {
		return nil, err
	}

	config := templates[idx]

	if config.Driver == "" {
		promptDriver := promptui.Prompt{
			Label: "Driver name",
		}
		config.Driver, err = promptDriver.Run()
		if err != nil {
			return nil, err
		}
		config.Name = config.Driver
	}

	helpText := getConnectionHelp(config.Driver)
	if helpText != "" {
		fmt.Println(helpText)
	}

	var promptURI promptui.Prompt
	if isParquetDriver(config.Driver) {
		promptURI = promptui.Prompt{
			Label: fmt.Sprintf("%s file path", config.Name),
		}
	} else if needsURIMask(config.Driver) {
		promptURI = promptui.Prompt{
			Label: fmt.Sprintf("%s connection URI", config.Name),
			Mask:  '*',
		}
	} else {
		promptURI = promptui.Prompt{
			Label: fmt.Sprintf("%s connection URI", config.Name),
		}
	}
	config.URI, err = promptURI.Run()
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func selectParquetIngestMode() (string, error) {
	fmt.Println("\n🔧 File Write Mode")

	modes := []struct {
		Name        string
		Description string
		Value       string
	}{
		{
			Name:        "Create",
			Description: "Create new file (fails if file already exists)",
			Value:       adbc.OptionValueIngestModeCreate,
		},
		{
			Name:        "Replace",
			Description: "Overwrite existing file",
			Value:       adbc.OptionValueIngestModeReplace,
		},
	}

	tpl := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "▶ {{ .Name | cyan }} - {{ .Description | faint }}",
		Inactive: "  {{ .Name | cyan }} - {{ .Description | faint }}",
		Selected: "✓ {{ .Name | green }}",
	}

	prompt := promptui.Select{
		Label:     "Select file write mode",
		Items:     modes,
		Templates: tpl,
	}

	idx, _, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return modes[idx].Value, nil
}

func selectIngestMode() (string, error) {
	fmt.Println("\n🔧 Table Ingest Mode")

	modes := []struct {
		Name        string
		Description string
		Value       string
	}{
		{
			Name:        "Create",
			Description: "Create new table (fails if exists)",
			Value:       adbc.OptionValueIngestModeCreate,
		},
		{
			Name:        "Append",
			Description: "Append to existing table (table must exist)",
			Value:       adbc.OptionValueIngestModeAppend,
		},
		{
			Name:        "Replace",
			Description: "Drop and recreate table",
			Value:       adbc.OptionValueIngestModeReplace,
		},
	}

	tpl := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "▶ {{ .Name | cyan }} - {{ .Description | faint }}",
		Inactive: "  {{ .Name | cyan }} - {{ .Description | faint }}",
		Selected: "✓ {{ .Name | green }}",
	}

	prompt := promptui.Select{
		Label:     "Select ingest mode",
		Items:     modes,
		Templates: tpl,
	}

	idx, _, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return modes[idx].Value, nil
}

func selectTransferMode() (TransferMode, error) {
	fmt.Println("\n⚡ Select Transfer Mode")

	modes := []struct {
		Name        string
		Description string
		Mode        TransferMode
	}{
		{
			Name:        "Batch",
			Description: "Process all data at once (good for smaller datasets)",
			Mode:        BatchMode,
		},
		{
			Name:        "Streaming",
			Description: "Process data in chunks (good for large datasets)",
			Mode:        StreamingMode,
		},
	}

	tpl := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "▶ {{ .Name | cyan }} - {{ .Description | faint }}",
		Inactive: "  {{ .Name | cyan }} - {{ .Description | faint }}",
		Selected: "✓ {{ .Name | green }}",
	}

	prompt := promptui.Select{
		Label:     "Select transfer mode",
		Items:     modes,
		Templates: tpl,
	}

	idx, _, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return modes[idx].Mode, nil
}

func getQuery() (string, error) {
	fmt.Println("\n📝 SQL Query")

	prompt := promptui.Prompt{
		Label:   "Enter SQL query to execute",
		Default: "SELECT * FROM your_table LIMIT 100",
	}

	query, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return query, nil
}

func getDestinationTable() (string, error) {
	fmt.Println("\n🎯 Destination Table")

	prompt := promptui.Prompt{
		Label:   "Enter destination table name",
		Default: "imported_data",
	}

	table, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return table, nil
}

func confirmExecution() bool {
	prompt := promptui.Prompt{
		Label:     "Proceed with transfer",
		IsConfirm: true,
	}

	result, err := prompt.Run()
	if err != nil {
		return false
	}

	return result == "y" || result == "Y"
}
