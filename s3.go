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
	"net/url"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func isS3Driver(driver string) bool {
	return strings.ToLower(driver) == "s3"
}

func parseS3URI(uri string) (bucket, key string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("invalid S3 URI %q: %w", uri, err)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid S3 URI %q: must start with s3://", uri)
	}
	bucket = u.Host
	if bucket == "" {
		return "", "", fmt.Errorf("invalid S3 URI %q: missing bucket name", uri)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("invalid S3 URI %q: missing object key (e.g. s3://my-bucket/path/output.parquet)", uri)
	}
	return bucket, key, nil
}

type s3WriteResult struct {
	rows    int64
	batches int
	err     error
}

// writeS3Dest streams an Arrow record reader to an S3 object as a Parquet file.
// ingestMode controls object creation behavior:
//   - create: fail if the object already exists
//   - replace: overwrite an existing object
//   - append: unsupported (S3 objects are immutable once written)
func writeS3Dest(stream array.RecordReader, s3URI string, ingestMode string) (*transferResult, error) {
	ctx := context.Background()

	if ingestMode == adbc.OptionValueIngestModeAppend {
		return nil, fmt.Errorf("append mode is not supported for S3 destinations: S3 objects are immutable once written; use --ingest-mode replace to overwrite")
	}

	bucket, key, err := parseS3URI(s3URI)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// For create mode, fail fast if the object already exists.
	if ingestMode == adbc.OptionValueIngestModeCreate {
		_, headErr := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if headErr == nil {
			return nil, fmt.Errorf("S3 object already exists: %s (use --ingest-mode replace to overwrite)", s3URI)
		}
		// Non-nil headErr means not found or no read permission — proceed and let the upload surface real issues.
	}

	pr, pw := io.Pipe()
	resultCh := make(chan s3WriteResult, 1)

	go func() {
		var res s3WriteResult
		defer func() {
			if res.err != nil {
				pw.CloseWithError(res.err)
			} else {
				pw.Close()
			}
			resultCh <- res
		}()

		schema := stream.Schema()
		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
		)

		fw, err := pqarrow.NewFileWriter(schema, pw, writerProps, pqarrow.DefaultWriterProps())
		if err != nil {
			res.err = fmt.Errorf("failed to create Parquet writer: %w", err)
			return
		}

		for stream.Next() {
			rec := stream.Record()
			logInfo("  Writing batch %d: %d rows...", res.batches+1, rec.NumRows())

			if err := fw.Write(rec); err != nil {
				rec.Release()
				res.err = fmt.Errorf("failed to write batch %d: %w", res.batches+1, err)
				return
			}

			res.rows += rec.NumRows()
			res.batches++
			logInfo(" ✓\n")
			rec.Release()
		}

		if err := stream.Err(); err != nil {
			res.err = fmt.Errorf("error reading source stream: %w", err)
			return
		}

		if err := fw.Close(); err != nil {
			res.err = fmt.Errorf("failed to finalize Parquet file: %w", err)
			return
		}
	}()

	uploader := s3manager.NewUploader(client)
	_, uploadErr := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   pr,
	})
	pr.Close()

	writeRes := <-resultCh

	if writeRes.err != nil {
		return nil, writeRes.err
	}
	if uploadErr != nil {
		return nil, fmt.Errorf("failed to upload to S3: %w", uploadErr)
	}

	logInfo("✓ Wrote %d batches, %d rows to %s\n", writeRes.batches, writeRes.rows, s3URI)
	return &transferResult{rows: writeRes.rows, batches: writeRes.batches}, nil
}
