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
	"encoding/json"
	"fmt"
	"os"
)

const (
	ExitSuccess            = 0
	ExitGeneralError       = 1
	ExitUsageError         = 2
	ExitSourceConnError    = 3
	ExitDestConnError      = 4
	ExitQueryError         = 5
	ExitMissingDriver      = 6
	ExitDriverInstallError = 7
)

type TransferResult struct {
	Status          string `json:"status"`
	RowsTransferred int64  `json:"rows_transferred"`
	Batches         int    `json:"batches,omitempty"`
	Source          DBInfo `json:"source"`
	Destination     DBInfo `json:"destination"`
	Table           string `json:"table"`
	IngestMode      string `json:"ingest_mode"`
	TransferMode    string `json:"transfer_mode"`
	Query           string `json:"query"`
	DurationMs      int64  `json:"duration_ms"`
}

type DBInfo struct {
	Driver string `json:"driver"`
	Name   string `json:"name"`
}

type ErrorResult struct {
	Error     string `json:"error"`
	ErrorCode int    `json:"error_code"`
}

type ConnectionTestResult struct {
	Status  string `json:"status"`
	Driver  string `json:"driver"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

type DriverListResult struct {
	Drivers []DriverInfo `json:"drivers"`
}

type DriverInfo struct {
	Name       string `json:"name"`
	Driver     string `json:"driver"`
	URIExample string `json:"uri_example"`
}

func isJSON() bool {
	return outputFormat == "json"
}

func isQuiet() bool {
	return outputFormat == "quiet"
}

func logInfo(format string, args ...interface{}) {
	if !isQuiet() && !isJSON() {
		fmt.Printf(format, args...)
	}
}

func logInfoln(msg string) {
	if !isQuiet() && !isJSON() {
		fmt.Println(msg)
	}
}

func outputJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

func exitWithError(msg string, code int) {
	if isJSON() {
		outputJSON(ErrorResult{Error: msg, ErrorCode: code})
	} else {
		fmt.Fprintf(os.Stderr, "Error: %s\n", msg)
	}
	os.Exit(code)
}
