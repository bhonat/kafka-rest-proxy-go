package api

import (
	"mime"
	"net/http"
	"strings"
)

const (
	mediaKafkaV2       = "application/vnd.kafka.v2+json"
	mediaKafkaJSONV2   = "application/vnd.kafka.json.v2+json"
	mediaKafkaBinaryV2 = "application/vnd.kafka.binary.v2+json"
	mediaJSON          = "application/json"
)

type payloadFormat int

const (
	formatJSON payloadFormat = iota
	formatBinary
)

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
		case "*/*", "application/*", mediaKafkaV2, mediaKafkaJSONV2, mediaKafkaBinaryV2, mediaJSON:
			return true
		}
	}
	return false
}
