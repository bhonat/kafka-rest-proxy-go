package api

import "testing"

func TestDecodeRejectsMissingValue(t *testing.T) {
	_, err := decodeProduceRequest("topic", []byte(`{"records":[{"key":"k"}]}`), formatJSON, 10)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeNullValueAsNil(t *testing.T) {
	records, err := decodeProduceRequest("topic", []byte(`{"records":[{"key":null,"value":null}]}`), formatJSON, 10)
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
