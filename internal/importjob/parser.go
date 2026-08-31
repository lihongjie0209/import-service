package importjob

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/xuri/excelize/v2"
)

var (
	ErrRowLimitExceeded  = errors.New("import row limit exceeded")
	ErrByteLimitExceeded = errors.New("import byte limit exceeded")
	ErrInvalidHeader     = errors.New("import file has an invalid header")
)

type ParsedBatch struct {
	Number         int64
	FirstRowNumber int64
	Rows           []map[string]any
}

type ParseResult struct {
	Rows, Bytes int64
	Checksum    string
}

func StreamRows(ctx context.Context, source io.Reader, format string, batchSize int, maxRows, maxBytes int64, consume func(ParsedBatch) error) (ParseResult, error) {
	if batchSize < 1 || maxRows < 1 || maxBytes < 1 {
		return ParseResult{}, errors.New("parser limits must be positive")
	}
	hash := sha256.New()
	limited := &limitReader{reader: io.TeeReader(source, hash), remaining: maxBytes}
	var rows int64
	emit := func(values []map[string]any) error {
		if len(values) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if rows+int64(len(values)) > maxRows {
			return ErrRowLimitExceeded
		}
		batch := ParsedBatch{Number: rows/int64(batchSize) + 1, FirstRowNumber: rows + 2, Rows: values}
		if err := consume(batch); err != nil {
			return err
		}
		rows += int64(len(values))
		return nil
	}
	var err error
	switch format {
	case FormatCSV:
		err = streamCSV(limited, batchSize, emit)
	case FormatJSONL:
		err = streamJSONL(limited, batchSize, emit)
	case FormatXLSX:
		err = streamXLSX(limited, batchSize, emit)
	default:
		err = fmt.Errorf("unsupported import format %q", format)
	}
	if limited.exceeded {
		return ParseResult{}, ErrByteLimitExceeded
	}
	if err != nil {
		return ParseResult{}, err
	}
	return ParseResult{Rows: rows, Bytes: limited.read, Checksum: hex.EncodeToString(hash.Sum(nil))}, nil
}

func streamCSV(source io.Reader, batchSize int, emit func([]map[string]any) error) error {
	reader := csv.NewReader(source)
	reader.FieldsPerRecord = -1
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read CSV header: %w", err)
	}
	if err := validateHeaders(headers); err != nil {
		return err
	}
	batch := make([]map[string]any, 0, batchSize)
	for {
		values, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read CSV row: %w", readErr)
		}
		if len(values) != len(headers) {
			return fmt.Errorf("CSV row has %d columns, want %d", len(values), len(headers))
		}
		row := make(map[string]any, len(headers))
		for i := range headers {
			row[headers[i]] = values[i]
		}
		batch = append(batch, row)
		if len(batch) == batchSize {
			if err := emit(batch); err != nil {
				return err
			}
			batch = make([]map[string]any, 0, batchSize)
		}
	}
	return emit(batch)
}

func streamJSONL(source io.Reader, batchSize int, emit func([]map[string]any) error) error {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	batch := make([]map[string]any, 0, batchSize)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return fmt.Errorf("decode JSONL row: %w", err)
		}
		if row == nil {
			return errors.New("JSONL row must be an object")
		}
		batch = append(batch, row)
		if len(batch) == batchSize {
			if err := emit(batch); err != nil {
				return err
			}
			batch = make([]map[string]any, 0, batchSize)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan JSONL: %w", err)
	}
	return emit(batch)
}

func streamXLSX(source io.Reader, batchSize int, emit func([]map[string]any) error) error {
	book, err := excelize.OpenReader(source)
	if err != nil {
		return fmt.Errorf("open XLSX: %w", err)
	}
	defer func() { _ = book.Close() }()
	sheets := book.GetSheetList()
	if len(sheets) == 0 {
		return ErrInvalidHeader
	}
	rows, err := book.Rows(sheets[0])
	if err != nil {
		return fmt.Errorf("read XLSX sheet: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return ErrInvalidHeader
	}
	headers, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read XLSX header: %w", err)
	}
	if err := validateHeaders(headers); err != nil {
		return err
	}
	batch := make([]map[string]any, 0, batchSize)
	for rows.Next() {
		values, rowErr := rows.Columns()
		if rowErr != nil {
			return fmt.Errorf("read XLSX row: %w", rowErr)
		}
		row := make(map[string]any, len(headers))
		for i := range headers {
			if i < len(values) {
				row[headers[i]] = values[i]
			} else {
				row[headers[i]] = ""
			}
		}
		batch = append(batch, row)
		if len(batch) == batchSize {
			if err := emit(batch); err != nil {
				return err
			}
			batch = make([]map[string]any, 0, batchSize)
		}
	}
	if err := rows.Error(); err != nil {
		return fmt.Errorf("iterate XLSX rows: %w", err)
	}
	return emit(batch)
}

func validateHeaders(values []string) error {
	if len(values) == 0 {
		return ErrInvalidHeader
	}
	seen := make(map[string]struct{}, len(values))
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
		if values[i] == "" {
			return ErrInvalidHeader
		}
		if _, ok := seen[values[i]]; ok {
			return fmt.Errorf("%w: duplicate column %q", ErrInvalidHeader, values[i])
		}
		seen[values[i]] = struct{}{}
	}
	return nil
}

type limitReader struct {
	reader    io.Reader
	remaining int64
	read      int64
	exceeded  bool
}

func (r *limitReader) Read(target []byte) (int, error) {
	if r.remaining <= 0 {
		var one [1]byte
		n, err := r.reader.Read(one[:])
		if n > 0 {
			r.exceeded = true
			return 0, ErrByteLimitExceeded
		}
		return 0, err
	}
	if int64(len(target)) > r.remaining {
		target = target[:r.remaining]
	}
	n, err := r.reader.Read(target)
	r.remaining -= int64(n)
	r.read += int64(n)
	return n, err
}
