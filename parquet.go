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
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func isParquetDriver(driver string) bool {
	return strings.ToLower(driver) == "parquet"
}

// writeParquetDest writes an Arrow record stream to a local Parquet file.
// ingestMode controls file creation behavior:
//   - create: fail if the file already exists
//   - replace: overwrite an existing file
//   - append: unsupported (Parquet files are immutable once written)
func writeParquetDest(stream array.RecordReader, filePath string, ingestMode string) (*transferResult, error) {
	var flags int
	switch ingestMode {
	case adbc.OptionValueIngestModeCreate:
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	case adbc.OptionValueIngestModeReplace:
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	case adbc.OptionValueIngestModeAppend:
		return nil, fmt.Errorf("append mode is not supported for Parquet destinations: Parquet files are immutable once written; use --ingest-mode replace to overwrite")
	default:
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	}

	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("Parquet file already exists: %s (use --ingest-mode replace to overwrite)", filePath)
		}
		return nil, fmt.Errorf("failed to create Parquet file %s: %w", filePath, err)
	}
	defer f.Close()

	schema := stream.Schema()

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)

	fw, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	rows := int64(0)
	batches := 0

	for stream.Next() {
		rec := stream.Record()
		logInfo("  Writing batch %d: %d rows...", batches+1, rec.NumRows())

		if err := fw.Write(rec); err != nil {
			rec.Release()
			fw.Close()
			return nil, fmt.Errorf("failed to write batch %d: %w", batches+1, err)
		}

		rows += rec.NumRows()
		batches++
		logInfo(" ✓\n")
		rec.Release()
	}

	if err := stream.Err(); err != nil {
		fw.Close()
		return nil, fmt.Errorf("error reading source stream: %w", err)
	}

	if err := fw.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize Parquet file: %w", err)
	}

	logInfo("✓ Wrote %d batches, %d rows to %s\n", batches, rows, filePath)
	return &transferResult{rows: rows, batches: batches}, nil
}
