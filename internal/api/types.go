package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

type nullableRaw struct {
	Present bool
	Raw     json.RawMessage
}

func (n *nullableRaw) UnmarshalJSON(b []byte) error {
	n.Present = true
	n.Raw = append(n.Raw[:0], b...)
	return nil
}

type rawProduceRequest struct {
	Records []rawRecord `json:"records"`
}

type rawRecord struct {
	Key       nullableRaw `json:"key"`
	Value     nullableRaw `json:"value"`
	Partition *int32      `json:"partition"`
	Headers   []rawHeader `json:"headers"`
}

type rawHeader struct {
	Key   string      `json:"key"`
	Value nullableRaw `json:"value"`
}

type produceResponse struct {
	Offsets []produceOffset `json:"offsets"`
}

type produceOffset struct {
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	ErrorCode *int16  `json:"error_code"`
	Error     *string `json:"error"`
}

type errorResponse struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func decodeProduceRequest(topic string, body []byte, format payloadFormat, maxRecords int) ([]producer.Record, error) {
	var req rawProduceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError{message: "invalid JSON request body: " + err.Error()}
	}
	if req.Records == nil {
		return nil, validationError{message: "request body must include records"}
	}
	if len(req.Records) > maxRecords {
		return nil, validationError{message: fmt.Sprintf("too many records: got %d, limit %d", len(req.Records), maxRecords)}
	}

	records := make([]producer.Record, 0, len(req.Records))
	for i, rr := range req.Records {
		if !rr.Value.Present {
			return nil, validationError{message: fmt.Sprintf("records[%d].value is required", i)}
		}

		key, err := decodeNullableValue(rr.Key, format)
		if err != nil {
			return nil, validationError{message: fmt.Sprintf("records[%d].key: %v", i, err)}
		}
		value, err := decodeNullableValue(rr.Value, format)
		if err != nil {
			return nil, validationError{message: fmt.Sprintf("records[%d].value: %v", i, err)}
		}
		headers, err := decodeHeaders(rr.Headers, format)
		if err != nil {
			return nil, validationError{message: fmt.Sprintf("records[%d].headers: %v", i, err)}
		}

		records = append(records, producer.Record{
			Topic:     topic,
			Key:       key,
			Value:     value,
			Headers:   headers,
			Partition: rr.Partition,
		})
	}

	return records, nil
}

func decodeHeaders(headers []rawHeader, format payloadFormat) ([]producer.Header, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	out := make([]producer.Header, 0, len(headers))
	for i, h := range headers {
		if h.Key == "" {
			return nil, fmt.Errorf("headers[%d].key is required", i)
		}
		if !h.Value.Present {
			return nil, fmt.Errorf("headers[%d].value is required", i)
		}
		v, err := decodeNullableValue(h.Value, format)
		if err != nil {
			return nil, fmt.Errorf("headers[%d].value: %w", i, err)
		}
		out = append(out, producer.Header{Key: h.Key, Value: v})
	}
	return out, nil
}

func decodeNullableValue(v nullableRaw, format payloadFormat) ([]byte, error) {
	if !v.Present || isJSONNull(v.Raw) {
		return nil, nil
	}

	switch format {
	case formatBinary:
		var encoded string
		if err := json.Unmarshal(v.Raw, &encoded); err != nil {
			return nil, fmt.Errorf("binary payloads must be base64 strings or null")
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid base64: %w", err)
		}
		return decoded, nil
	default:
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, v.Raw); err != nil {
			return nil, err
		}
		return compacted.Bytes(), nil
	}
}

func isJSONNull(raw json.RawMessage) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}

func responseFromResults(results []producer.Result) produceResponse {
	resp := produceResponse{Offsets: make([]produceOffset, len(results))}
	for i, r := range results {
		var errText *string
		if r.Err != nil {
			s := r.Err.Error()
			errText = &s
		}
		resp.Offsets[i] = produceOffset{
			Partition: r.Partition,
			Offset:    r.Offset,
			ErrorCode: r.ErrorCode,
			Error:     errText,
		}
	}
	return resp
}

func countResultStatus(results []producer.Result) (successes, failures int) {
	for _, r := range results {
		if r.Err != nil || r.ErrorCode != nil {
			failures++
		} else {
			successes++
		}
	}
	return successes, failures
}
