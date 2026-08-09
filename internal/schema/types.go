package schema

import (
	"encoding/json"
	"strings"
)

const (
	TypeBinary     = "BINARY"
	TypeJSON       = "JSON"
	TypeString     = "STRING"
	TypeAvro       = "AVRO"
	TypeJSONSchema = "JSONSCHEMA"
	TypeProtobuf   = "PROTOBUF"

	StrategyTopicName       = "TOPIC_NAME"
	StrategyRecordName      = "RECORD_NAME"
	StrategyTopicRecordName = "TOPIC_RECORD_NAME"
)

// RequestData is the Confluent v3 producer key/value envelope. The v2
// schema-aware media types are converted into this same shape internally.
type RequestData struct {
	Type                string          `json:"type,omitempty"`
	Subject             string          `json:"subject,omitempty"`
	SubjectNameStrategy string          `json:"subject_name_strategy,omitempty"`
	SchemaID            *int            `json:"schema_id,omitempty"`
	SchemaVersion       *int            `json:"schema_version,omitempty"`
	Schema              string          `json:"schema,omitempty"`
	Data                json.RawMessage `json:"data,omitempty"`
	DataPresent         bool            `json:"-"`
}

func (d *RequestData) UnmarshalJSON(b []byte) error {
	type alias RequestData
	var raw struct {
		alias
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*d = RequestData(raw.alias)
	if raw.Data != nil {
		d.DataPresent = true
		d.Data = append(d.Data[:0], raw.Data...)
	}
	return nil
}

func (d RequestData) NormalizedType() string {
	return strings.ToUpper(strings.TrimSpace(d.Type))
}

func (d RequestData) IsSchemaAware() bool {
	t := d.NormalizedType()
	return t == TypeAvro ||
		t == TypeJSONSchema ||
		t == TypeProtobuf ||
		d.Schema != "" ||
		d.SchemaID != nil ||
		d.SchemaVersion != nil
}

type Metadata struct {
	Type          string
	Size          int
	Subject       string
	SchemaID      *int
	SchemaVersion *int
}

type ResolveRequest struct {
	Subject       string
	Type          string
	SchemaID      *int
	SchemaVersion *int
	Schema        string
}

type Resolved struct {
	Subject       string
	Type          string
	SchemaID      int
	SchemaVersion *int
	Schema        string
}
