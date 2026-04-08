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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var knownDrivers = []DriverInfo{
	{Name: "PostgreSQL", Driver: "postgresql", URIExample: "postgresql://user:password@host:port/database"},
	{Name: "SQLite", Driver: "sqlite", URIExample: "file:path/to/database.db"},
	{Name: "DuckDB", Driver: "duckdb", URIExample: "path/to/database.duckdb"},
	{Name: "BigQuery", Driver: "bigquery", URIExample: "bigquery://project-id/dataset"},
	{Name: "MotherDuck", Driver: "motherduck", URIExample: "md:database_name"},
	{Name: "Snowflake", Driver: "snowflake", URIExample: "snowflake://user:pass@account/database"},
	{Name: "Flight SQL", Driver: "flightsql", URIExample: "grpc://host:port"},
	{Name: "Parquet File", Driver: "parquet", URIExample: "path/to/output.parquet (built-in, destination only)"},
}

var listDriversCmd = &cobra.Command{
	Use:   "list-drivers",
	Short: "List supported database drivers",
	Long:  "List all known ADBC database drivers with example connection URIs.",
	Run: func(cmd *cobra.Command, args []string) {
		if isJSON() {
			outputJSON(DriverListResult{Drivers: knownDrivers})
			return
		}

		fmt.Println("Supported ADBC Drivers:")
		fmt.Println()
		for _, d := range knownDrivers {
			fmt.Printf("  %-14s  driver: %-14s  example: %s\n", d.Name, d.Driver, d.URIExample)
		}
		fmt.Println()
		fmt.Println("Use any ADBC-compatible driver by specifying its name with --source-driver or --dest-driver.")
	},
}

func isMissingDriverError(err error) (bool, string) {
	if err == nil {
		return false, ""
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "Could not load") {
		if strings.Contains(errMsg, "Could not load `") {
			start := strings.Index(errMsg, "Could not load `") + 16
			end := strings.Index(errMsg[start:], "`")
			if end > 0 {
				driverName := errMsg[start : start+end]
				return true, driverName
			}
		}
		return true, ""
	}
	return false, ""
}

func confirmDriverInstallation(driverName string) bool {
	fmt.Printf("\n⚠️  Missing ADBC Driver Detected\n")
	fmt.Printf("Driver '%s' is not installed on your system.\n\n", driverName)

	prompt := promptui.Prompt{
		Label:     "Would you like to install the missing driver automatically",
		IsConfirm: true,
	}

	result, err := prompt.Run()
	if err != nil {
		return false
	}

	return result == "y" || result == "Y"
}

func installADBCDrivers(driverName string) error {
	logInfoln("\n📦 Installing ADBC Driver...")

	osType := runtime.GOOS
	arch := runtime.GOARCH

	switch osType {
	case "darwin":
		return installDriversMacOS(driverName)
	case "linux":
		return installDriversLinux(arch)
	case "windows":
		return installDriversWindows()
	default:
		return fmt.Errorf("unsupported operating system: %s", osType)
	}
}

func installDriversMacOS(driverName string) error {
	if isCommandAvailable("dbc") {
		logInfoln("  ✓ dbc tool found!")
		return installDriverViaDbc(driverName)
	}

	logInfoln("  dbc tool not found. dbc is the recommended way to install ADBC drivers.")
	logInfoln("  It's faster and more lightweight than installing the full Apache Arrow suite.")

	prompt := promptui.Prompt{
		Label:     "Would you like to install dbc (recommended)",
		IsConfirm: true,
		Default:   "y",
	}

	result, err := prompt.Run()
	if err == nil && (result == "y" || result == "Y" || result == "") {
		if err := installDbcTool(); err != nil {
			logInfo("  ⚠️  Failed to install dbc: %v\n", err)
			logInfoln("  Falling back to Apache Arrow installation...")
		} else {
			return installDriverViaDbc(driverName)
		}
	}

	return installViaApacheArrow()
}

func installDriversMacOSNonInteractive(driverName string) error {
	if isCommandAvailable("dbc") {
		logInfoln("  ✓ dbc tool found!")
		return installDriverViaDbc(driverName)
	}

	logInfoln("  dbc not found, attempting to install dbc first...")
	if err := installDbcTool(); err != nil {
		logInfo("  ⚠️  Failed to install dbc: %v\n", err)
		logInfoln("  Falling back to Apache Arrow installation...")
		return installViaApacheArrow()
	}
	return installDriverViaDbc(driverName)
}

func installDbcTool() error {
	logInfoln("\n  Installing dbc tool via Homebrew...")

	if !isCommandAvailable("brew") {
		return fmt.Errorf("homebrew not found")
	}

	logInfoln("  $ brew tap columnar-tech/tap")
	cmd := exec.Command("brew", "tap", "columnar-tech/tap")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to add columnar-tech tap: %w", err)
	}

	logInfoln("  $ brew install --cask dbc")
	cmd = exec.Command("brew", "install", "--cask", "dbc")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to install dbc: %w", err)
	}

	logInfoln("\n  ✓ dbc tool installed successfully!")
	return nil
}

func installDriverViaDbc(driverName string) error {
	logInfo("\n  Installing %s driver via dbc...\n", driverName)
	logInfo("  $ dbc install %s\n\n", driverName)

	cmd := exec.Command("dbc", "install", driverName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to install %s driver via dbc: %w", driverName, err)
	}

	logInfo("\n✓ %s driver installed successfully via dbc!\n", driverName)
	logInfoln("\nNote: If the driver is still not found, you may need to restart your terminal")
	logInfoln("or set the ADBC_DRIVER_PATH environment variable.")

	return nil
}

func installViaApacheArrow() error {
	logInfoln("\n  Installing Apache Arrow with ADBC drivers via Homebrew...")
	logInfoln("  This installs all ADBC drivers but may take several minutes...")
	logInfoln("")

	if !isCommandAvailable("brew") {
		logInfoln("\n❌ Homebrew is not installed.")
		logInfoln("\nTo install ADBC drivers manually:")
		logInfoln("1. Install Homebrew: https://brew.sh")
		logInfoln("2. Install dbc: brew tap columnar-tech/tap && brew install --cask dbc")
		logInfoln("3. Or install Apache Arrow: brew install apache-arrow")
		logInfoln("4. Or build from source: https://github.com/apache/arrow-adbc")
		return fmt.Errorf("homebrew not found")
	}

	cmd := exec.Command("brew", "install", "apache-arrow")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to install via Homebrew: %w", err)
	}

	homeDir, err := os.UserHomeDir()
	if err == nil {
		adbcPath := filepath.Join(homeDir, "Library", "Application Support", "ADBC", "Drivers")
		os.MkdirAll(adbcPath, 0755)
		logInfo("\n✓ ADBC driver directory created: %s\n", adbcPath)
	}

	logInfoln("\n✓ Apache Arrow with ADBC drivers installed successfully!")
	logInfoln("\nYou may need to restart the terminal or run:")
	logInfoln("  export ADBC_DRIVER_PATH=\"$(brew --prefix)/lib\"")

	return nil
}

func installDriversLinux(arch string) error {
	logInfoln("\n📋 Manual Installation Required for Linux")
	logInfoln("\nOption 1: Install from package manager")
	logInfoln("  Ubuntu/Debian:")
	logInfoln("    sudo apt install -y apache-arrow-adbc")
	logInfoln("\n  Fedora/RHEL:")
	logInfoln("    sudo dnf install apache-arrow-adbc")
	logInfoln("\nOption 2: Build from source")
	logInfoln("  git clone https://github.com/apache/arrow-adbc.git")
	logInfoln("  cd arrow-adbc")
	logInfoln("  # Follow build instructions at: https://arrow.apache.org/adbc/")
	logInfoln("\nAfter installation, set ADBC_DRIVER_PATH:")
	logInfoln("  export ADBC_DRIVER_PATH=\"/usr/local/lib\"")

	return fmt.Errorf("manual installation required")
}

func installDriversWindows() error {
	logInfoln("\n📋 Manual Installation Required for Windows")
	logInfoln("\nOption 1: Use vcpkg")
	logInfoln("  vcpkg install arrow:x64-windows")
	logInfoln("\nOption 2: Build from source")
	logInfoln("  git clone https://github.com/apache/arrow-adbc.git")
	logInfoln("  cd arrow-adbc")
	logInfoln("  # Follow build instructions at: https://arrow.apache.org/adbc/")
	logInfoln("\nAfter installation, set ADBC_DRIVER_PATH environment variable")

	return fmt.Errorf("manual installation required")
}

func isCommandAvailable(command string) bool {
	_, err := exec.LookPath(command)
	return err == nil
}

// handleMissingDriver processes a missing driver error with appropriate behavior
// based on the autoInstall and noInstall flags. Returns nil if resolved, error otherwise.
func handleMissingDriver(driverName string, autoInstall, noInstall, interactive bool) error {
	if noInstall {
		return fmt.Errorf("missing driver: %s (driver installation disabled via --no-install-drivers)", driverName)
	}

	if autoInstall {
		logInfo("\n⚠️  Missing driver '%s', auto-installing...\n", driverName)
		if runtime.GOOS == "darwin" {
			return installDriversMacOSNonInteractive(driverName)
		}
		return installADBCDrivers(driverName)
	}

	if interactive {
		if confirmDriverInstallation(driverName) {
			return installADBCDrivers(driverName)
		}
		return fmt.Errorf("driver installation declined by user")
	}

	return fmt.Errorf("missing driver: %s (use --auto-install-drivers to install automatically)", driverName)
}
