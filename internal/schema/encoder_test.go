package schema

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
)

func TestEncoderAvroAddsConfluentWireFormat(t *testing.T) {
	reg := NewMemoryRegistry()
	enc := NewEncoder(reg)
	schemaText := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	payload, meta, err := enc.Encode(context.Background(), "orders", false, &RequestData{
		Type:        TypeAvro,
		Schema:      schemaText,
		Data:        json.RawMessage(`{"id":"a-1"}`),
		DataPresent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertWireHeader(t, payload, 1)
	if got, want := payload[5:], []byte{6, 'a', '-', '1'}; !bytes.Equal(got, want) {
		t.Fatalf("avro payload = %x, want %x", got, want)
	}
	if meta.Type != TypeAvro || meta.SchemaID == nil || *meta.SchemaID != 1 || meta.Subject != "orders-value" {
		t.Fatalf("metadata = %+v", meta)
	}
}

func TestEncoderJSONSchemaValidatesAndFramesJSON(t *testing.T) {
	enc := NewEncoder(NewMemoryRegistry())
	schemaText := `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`
	payload, meta, err := enc.Encode(context.Background(), "orders", false, &RequestData{
		Type:        TypeJSONSchema,
		Schema:      schemaText,
		Data:        json.RawMessage(`{"id":"a-1"}`),
		DataPresent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertWireHeader(t, payload, 1)
	if !bytes.Contains(payload[5:], []byte(`"id":"a-1"`)) {
		t.Fatalf("payload does not contain compact JSON: %x", payload)
	}
	if meta.Type != TypeJSONSchema {
		t.Fatalf("type = %q", meta.Type)
	}
}

func TestEncoderJSONSchemaRejectsInvalidData(t *testing.T) {
	enc := NewEncoder(NewMemoryRegistry())
	_, _, err := enc.Encode(context.Background(), "orders", false, &RequestData{
		Type:        TypeJSONSchema,
		Schema:      `{"type":"object","required":["id"]}`,
		Data:        json.RawMessage(`{}`),
		DataPresent: true,
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestEncoderProtobufAddsMessageIndexAndPayload(t *testing.T) {
	enc := NewEncoder(NewMemoryRegistry())
	protoSchema := `syntax = "proto3"; message Order { string id = 1; }`
	payload, meta, err := enc.Encode(context.Background(), "orders", false, &RequestData{
		Type:        TypeProtobuf,
		Schema:      protoSchema,
		Data:        json.RawMessage(`{"id":"a-1"}`),
		DataPresent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertWireHeader(t, payload, 1)
	if len(payload) < 7 || payload[5] != 0 {
		t.Fatalf("protobuf payload missing first-message index byte: %x", payload)
	}
	if meta.Type != TypeProtobuf {
		t.Fatalf("type = %q", meta.Type)
	}
}

func assertWireHeader(t *testing.T, payload []byte, wantID int) {
	t.Helper()
	if len(payload) < 6 {
		t.Fatalf("payload too short: %x", payload)
	}
	if payload[0] != 0 {
		t.Fatalf("magic byte = %d, want 0", payload[0])
	}
	if got := int(binary.BigEndian.Uint32(payload[1:5])); got != wantID {
		t.Fatalf("schema id = %d, want %d", got, wantID)
	}
}
