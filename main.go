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

	"github.com/spf13/cobra"
)

var (
	Version      = "0.1.0"
	outputFormat string
)

var rootCmd = &cobra.Command{
	Use:   "fletch",
	Short: "Fletch - ADBC Data Transfer CLI",
	Long: `Fletch is a command-line tool for transferring data between databases using
Apache Arrow Database Connectivity (ADBC).

Supports both interactive and non-interactive (agent-friendly) modes.
Run 'fletch transfer' without flags for the interactive wizard, or
provide all required flags for scripted and automated use.

Subcommands:
  transfer         Transfer data between databases
  test-connection  Validate connectivity to a database
  list-drivers     List supported database drivers
  version          Print version information

Global Flags:
  -o, --output     Output format: text (default), json, quiet`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		if isJSON() {
			outputJSON(map[string]string{"version": Version})
		} else {
			fmt.Printf("fletch version %s\n", Version)
		}
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "text", "Output format: text, json, quiet")

	rootCmd.AddCommand(transferCmd)
	rootCmd.AddCommand(testConnectionCmd)
	rootCmd.AddCommand(listDriversCmd)
	rootCmd.AddCommand(versionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(ExitUsageError)
	}
}
