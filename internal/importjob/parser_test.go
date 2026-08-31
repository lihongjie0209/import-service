package importjob

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestStreamRowsCSVUsesBoundedBatches(t *testing.T) {
	var batches []ParsedBatch
	result, err := StreamRows(context.Background(), strings.NewReader("id,name\n1,A\n2,B\n3,C\n"), FormatCSV, 2, 10, 1024, func(batch ParsedBatch) error {
		batches = append(batches, batch)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 3 || len(result.Checksum) != 64 || len(batches) != 2 || batches[0].FirstRowNumber != 2 || batches[1].Rows[0]["name"] != "C" {
		t.Fatalf("result=%+v batches=%+v", result, batches)
	}
}

func TestStreamRowsJSONLAndLimits(t *testing.T) {
	input := "{\"id\":1}\n{\"id\":2}\n"
	result, err := StreamRows(context.Background(), strings.NewReader(input), FormatJSONL, 10, 10, 1024, func(ParsedBatch) error { return nil })
	if err != nil || result.Rows != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	_, err = StreamRows(context.Background(), strings.NewReader(input), FormatJSONL, 10, 1, 1024, func(ParsedBatch) error { return nil })
	if !errors.Is(err, ErrRowLimitExceeded) {
		t.Fatalf("row limit error=%v", err)
	}
	_, err = StreamRows(context.Background(), strings.NewReader(input), FormatJSONL, 10, 10, 3, func(ParsedBatch) error { return nil })
	if !errors.Is(err, ErrByteLimitExceeded) {
		t.Fatalf("byte limit error=%v", err)
	}
}

func TestStreamRowsRejectsDuplicateHeaders(t *testing.T) {
	_, err := StreamRows(context.Background(), strings.NewReader("id,id\n1,2\n"), FormatCSV, 10, 10, 1024, func(ParsedBatch) error { return nil })
	if !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("error=%v", err)
	}
}
