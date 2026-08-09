package api

import (
	"errors"
	"testing"
)

func TestDecodeMissingValueAsNil(t *testing.T) {
	records, err := decodeProduceRequest("topic", []byte(`{"records":[{"key":"k"}]}`), formatJSON, decodeLimits{MaxRecords: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	if records[0].Value != nil {
		t.Fatalf("value = %#v, want nil", records[0].Value)
	}
}

func TestDecodeNullValueAsNil(t *testing.T) {
	records, err := decodeProduceRequest("topic", []byte(`{"records":[{"key":null,"value":null}]}`), formatJSON, decodeLimits{MaxRecords: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	if records[0].Key != nil {
		t.Fatalf("key = %#v, want nil", records[0].Key)
	}
	if records[0].Value != nil {
		t.Fatalf("value = %#v, want nil", records[0].Value)
	}
}

func TestDecodeRejectsMissingRecordsAsUnprocessable(t *testing.T) {
	_, err := decodeProduceRequest("topic", []byte(`{}`), formatJSON, decodeLimits{MaxRecords: 10})
	if err == nil {
		t.Fatal("expected error")
	}
	var got unprocessableError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %[1]v, want unprocessableError", err)
	}
}

func TestDecodeRejectsEmptyRecordsAsUnprocessable(t *testing.T) {
	_, err := decodeProduceRequest("topic", []byte(`{"records":[]}`), formatJSON, decodeLimits{MaxRecords: 10})
	if err == nil {
		t.Fatal("expected error")
	}
	var got unprocessableError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %[1]v, want unprocessableError", err)
	}
}

func TestDecodeRejectsOversizedRecord(t *testing.T) {
	_, err := decodeProduceRequest("topic", []byte(`{"records":[{"value":"abcdef"}]}`), formatJSON, decodeLimits{
		MaxRecords:     10,
		MaxRecordBytes: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeRejectsTooManyRecords(t *testing.T) {
	_, err := decodeProduceRequest("topic", []byte(`{"records":[{"value":1},{"value":2}]}`), formatJSON, decodeLimits{
		MaxRecords: 1,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeRejectsNegativePartition(t *testing.T) {
	_, err := decodeProduceRequest("topic", []byte(`{"records":[{"partition":-1,"value":{"ok":true}}]}`), formatJSON, decodeLimits{
		MaxRecords: 10,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "records[0].partition must be non-negative"; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestDecodeAppliesForcedPartition(t *testing.T) {
	partition := int32(3)
	records, err := decodeProduceRequest("topic", []byte(`{"records":[{"partition":9,"value":{"ok":true}}]}`), formatJSON, decodeLimits{
		MaxRecords: 10,
	}, &partition)
	if err != nil {
		t.Fatal(err)
	}
	if records[0].Partition == nil || *records[0].Partition != 3 {
		t.Fatalf("partition = %v, want forced partition 3", records[0].Partition)
	}
}

func TestDecodeJSONValueKeepsCompactPayloadBorrowed(t *testing.T) {
	raw := []byte(`{"x":"hello world","escaped":"quote: \"","arr":[1,2,3]}`)
	got, err := decodeJSONValue(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(raw) {
		t.Fatalf("got %s, want %s", got, raw)
	}
	if len(got) > 0 && &got[0] != &raw[0] {
		t.Fatal("compact JSON should be borrowed, not copied")
	}
}

func TestDecodeJSONValueCompactsPrettyPayload(t *testing.T) {
	got, err := decodeJSONValue([]byte("{\n  \"x\": \"hello world\",\n  \"arr\": [1, 2]\n}"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"x":"hello world","arr":[1,2]}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}
