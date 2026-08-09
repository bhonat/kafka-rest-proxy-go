package schema

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/linkedin/goavro/v2"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Encoder struct {
	Registry Registry
}

func NewEncoder(registry Registry) *Encoder {
	return &Encoder{Registry: registry}
}

func (e *Encoder) Encode(ctx context.Context, topic string, key bool, data *RequestData) ([]byte, Metadata, error) {
	if data == nil {
		return nil, Metadata{}, nil
	}
	typ := data.NormalizedType()
	if typ == "" && data.IsSchemaAware() {
		typ = TypeAvro
	}
	if typ == "" {
		typ = TypeJSON
	}

	var payload []byte
	var err error
	var meta Metadata
	meta.Type = typ

	switch typ {
	case TypeBinary:
		payload, err = encodeBinary(data)
	case TypeString:
		payload, err = encodeString(data)
	case TypeJSON:
		payload, err = encodeJSON(data)
	case TypeAvro, TypeJSONSchema, TypeProtobuf:
		payload, meta, err = e.encodeSchemaAware(ctx, topic, key, typ, data)
	default:
		return nil, Metadata{}, fmt.Errorf("unsupported record data type %q", data.Type)
	}
	if err != nil {
		return nil, Metadata{}, err
	}
	if meta.Type == "" {
		meta.Type = typ
	}
	meta.Size = len(payload)
	return payload, meta, nil
}

func encodeBinary(data *RequestData) ([]byte, error) {
	if !data.DataPresent || isJSONNull(data.Data) {
		return nil, nil
	}
	var encoded string
	if err := json.Unmarshal(data.Data, &encoded); err != nil {
		return nil, fmt.Errorf("BINARY data must be a base64 string")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("BINARY data is not valid base64")
	}
	return decoded, nil
}

func encodeString(data *RequestData) ([]byte, error) {
	if !data.DataPresent || isJSONNull(data.Data) {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(data.Data, &s); err != nil {
		return nil, fmt.Errorf("STRING data must be a string")
	}
	return []byte(s), nil
}

func encodeJSON(data *RequestData) ([]byte, error) {
	if !data.DataPresent || isJSONNull(data.Data) {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, data.Data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *Encoder) encodeSchemaAware(ctx context.Context, topic string, key bool, typ string, data *RequestData) ([]byte, Metadata, error) {
	if !data.DataPresent || isJSONNull(data.Data) {
		return nil, Metadata{Type: typ}, nil
	}
	if e == nil || e.Registry == nil {
		return nil, Metadata{}, fmt.Errorf("schema registry is required for %s records", typ)
	}

	subject := resolveSubject(topic, key, *data, typ)
	resolved, err := e.Registry.Resolve(ctx, ResolveRequest{
		Subject:       subject,
		Type:          typ,
		SchemaID:      data.SchemaID,
		SchemaVersion: data.SchemaVersion,
		Schema:        data.Schema,
	})
	if err != nil {
		return nil, Metadata{}, err
	}
	if resolved.Type != "" {
		typ = resolved.Type
	}

	var encoded []byte
	switch typ {
	case TypeAvro:
		encoded, err = encodeAvro(resolved.Schema, data.Data)
	case TypeJSONSchema:
		encoded, err = encodeJSONSchema(resolved.Schema, data.Data)
	case TypeProtobuf:
		encoded, err = encodeProtobuf(resolved.Schema, data.Data)
	default:
		err = fmt.Errorf("unsupported schema type %q", typ)
	}
	if err != nil {
		return nil, Metadata{}, err
	}

	wire := appendConfluentWireFormat(nil, resolved.SchemaID, typ, encoded)
	return wire, Metadata{
		Type:          typ,
		Size:          len(wire),
		Subject:       firstNonEmpty(resolved.Subject, subject),
		SchemaID:      &resolved.SchemaID,
		SchemaVersion: resolved.SchemaVersion,
	}, nil
}

func encodeAvro(schemaText string, raw json.RawMessage) ([]byte, error) {
	codec, err := goavro.NewCodec(schemaText)
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromTextual(raw)
	if err != nil {
		return nil, err
	}
	return codec.BinaryFromNative(nil, native)
}

func encodeJSONSchema(schemaText string, raw json.RawMessage) ([]byte, error) {
	var doc any
	dec := json.NewDecoder(strings.NewReader(schemaText))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", doc); err != nil {
		return nil, err
	}
	sch, err := compiler.Compile("schema.json")
	if err != nil {
		return nil, err
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	if err := sch.Validate(instance); err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, err
	}
	return compact.Bytes(), nil
}

func encodeProtobuf(schemaText string, raw json.RawMessage) ([]byte, error) {
	fileName := "schema.proto"
	compiler := protocompile.Compiler{
		Resolver: protocompile.ResolverFunc(func(path string) (protocompile.SearchResult, error) {
			if path != fileName {
				return protocompile.SearchResult{}, protoregistry.NotFound
			}
			return protocompile.SearchResult{Source: io.NopCloser(strings.NewReader(schemaText))}, nil
		}),
		MaxParallelism: 1,
	}
	files, err := compiler.Compile(context.Background(), fileName)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 || files[0].Messages().Len() == 0 {
		return nil, fmt.Errorf("protobuf schema must define at least one message")
	}
	msgDesc := firstMessage(files[0].Messages())
	msg := dynamicpb.NewMessage(msgDesc)
	if err := protojson.Unmarshal(raw, msg); err != nil {
		return nil, err
	}
	return proto.Marshal(msg)
}

func firstMessage(messages protoreflect.MessageDescriptors) protoreflect.MessageDescriptor {
	msg := messages.Get(0)
	for msg.Messages().Len() > 0 {
		msg = msg.Messages().Get(0)
	}
	return msg
}

func appendConfluentWireFormat(dst []byte, schemaID int, typ string, payload []byte) []byte {
	dst = append(dst, 0)
	var id [4]byte
	binary.BigEndian.PutUint32(id[:], uint32(schemaID))
	dst = append(dst, id[:]...)
	if typ == TypeProtobuf {
		// Confluent Protobuf serialization stores message indexes between the
		// schema id and payload. For the common first-message case, the index
		// array [0] is optimized to a single zero byte.
		dst = append(dst, 0)
	}
	return append(dst, payload...)
}

func resolveSubject(topic string, key bool, data RequestData, typ string) string {
	if strings.TrimSpace(data.Subject) != "" {
		return strings.TrimSpace(data.Subject)
	}
	strategy := strings.ToUpper(strings.TrimSpace(data.SubjectNameStrategy))
	if strategy == "" {
		strategy = StrategyTopicName
	}
	suffix := "value"
	if key {
		suffix = "key"
	}
	switch strategy {
	case StrategyRecordName:
		if name := schemaRecordName(typ, data.Schema); name != "" {
			return name
		}
	case StrategyTopicRecordName:
		if name := schemaRecordName(typ, data.Schema); name != "" {
			return topic + "-" + name
		}
	}
	return topic + "-" + suffix
}

func schemaRecordName(typ, schemaText string) string {
	switch normalizeSchemaType(typ) {
	case TypeAvro:
		var doc struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		}
		_ = json.Unmarshal([]byte(schemaText), &doc)
		if doc.Name == "" {
			return ""
		}
		if doc.Namespace != "" {
			return doc.Namespace + "." + doc.Name
		}
		return doc.Name
	case TypeJSONSchema:
		var doc struct {
			Title string `json:"title"`
		}
		_ = json.Unmarshal([]byte(schemaText), &doc)
		return doc.Title
	case TypeProtobuf:
		re := regexp.MustCompile(`(?m)^\s*message\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{`)
		match := re.FindStringSubmatch(schemaText)
		if len(match) == 2 {
			return match[1]
		}
	}
	return ""
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
