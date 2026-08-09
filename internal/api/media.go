package api

import (
	"mime"
	"net/http"
	"strings"
)

const (
	mediaKafkaV2         = "application/vnd.kafka.v2+json"
	mediaKafkaJSONV2     = "application/vnd.kafka.json.v2+json"
	mediaKafkaBinaryV2   = "application/vnd.kafka.binary.v2+json"
	mediaKafkaAvroV2     = "application/vnd.kafka.avro.v2+json"
	mediaKafkaProtobufV2 = "application/vnd.kafka.protobuf.v2+json"
	mediaKafkaJSONSchV2  = "application/vnd.kafka.jsonschema.v2+json"
	mediaJSON            = "application/json"
)

type payloadFormat int

const (
	formatJSON payloadFormat = iota
	formatBinary
	formatAvro
	formatProtobuf
	formatJSONSchema
)

func (f payloadFormat) String() string {
	switch f {
	case formatBinary:
		return "binary"
	case formatAvro:
		return "avro"
	case formatProtobuf:
		return "protobuf"
	case formatJSONSchema:
		return "jsonschema"
	default:
		return "json"
	}
}

func (f payloadFormat) schemaType() string {
	switch f {
	case formatAvro:
		return "AVRO"
	case formatProtobuf:
		return "PROTOBUF"
	case formatJSONSchema:
		return "JSONSCHEMA"
	default:
		return ""
	}
}

func parseContentType(header string) (payloadFormat, bool) {
	if strings.TrimSpace(header) == "" {
		return formatJSON, true
	}
	mt, _, err := mime.ParseMediaType(header)
	if err != nil {
		return formatJSON, false
	}
	switch strings.ToLower(mt) {
	case mediaKafkaJSONV2, mediaKafkaV2, mediaJSON:
		return formatJSON, true
	case mediaKafkaBinaryV2:
		return formatBinary, true
	case mediaKafkaAvroV2:
		return formatAvro, true
	case mediaKafkaProtobufV2:
		return formatProtobuf, true
	case mediaKafkaJSONSchV2:
		return formatJSONSchema, true
	default:
		return formatJSON, false
	}
}

func acceptsResponse(r *http.Request) bool {
	accept := strings.TrimSpace(r.Header.Get("Accept"))
	if accept == "" {
		return true
	}
	for _, part := range strings.Split(accept, ",") {
		mt, _, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		switch strings.ToLower(mt) {
		case "*/*", "application/*", mediaKafkaV2, mediaKafkaJSONV2, mediaKafkaBinaryV2, mediaKafkaAvroV2, mediaKafkaProtobufV2, mediaKafkaJSONSchV2, mediaJSON:
			return true
		}
	}
	return false
}
