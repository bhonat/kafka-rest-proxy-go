package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

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

type decodeLimits struct {
	MaxRecords     int
	MaxRecordBytes int64
	MaxKeyBytes    int64
	MaxHeaders     int
	MaxHeaderBytes int64
}

type produceResponse struct {
	Offsets       []produceOffset `json:"offsets"`
	KeySchemaID   *int            `json:"key_schema_id"`
	ValueSchemaID *int            `json:"value_schema_id"`
}

type produceOffset struct {
	Partition *int32  `json:"partition"`
	Offset    *int64  `json:"offset"`
	ErrorCode *int    `json:"error_code"`
	Error     *string `json:"error"`
}

type errorResponse struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func decodeProduceRequest(topic string, body []byte, format payloadFormat, limits decodeLimits) ([]producer.Record, error) {
	limits = limits.withDefaults()

	var req rawProduceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError{message: err.Error()}
	}
	if req.Records == nil {
		return nil, validationError{message: "request body must include records"}
	}
	if len(req.Records) > limits.MaxRecords {
		return nil, validationError{message: fmt.Sprintf("too many records: got %d, limit %d", len(req.Records), limits.MaxRecords)}
	}

	records := make([]producer.Record, 0, len(req.Records))
	for i, rr := range req.Records {
		if !rr.Value.Present {
			return nil, validationError{message: fmt.Sprintf("records[%d].value is required", i)}
		}

		key, err := decodeNullableValue(rr.Key, format)
		if err != nil {
			var be binaryDecodeError
			if errors.As(err, &be) {
				return nil, validationError{message: be.Error()}
			}
			return nil, validationError{message: fmt.Sprintf("records[%d].key: %v", i, err)}
		}
		value, err := decodeNullableValue(rr.Value, format)
		if err != nil {
			var be binaryDecodeError
			if errors.As(err, &be) {
				return nil, validationError{message: be.Error()}
			}
			return nil, validationError{message: fmt.Sprintf("records[%d].value: %v", i, err)}
		}
		headers, err := decodeHeaders(rr.Headers, format, limits)
		if err != nil {
			return nil, validationError{message: fmt.Sprintf("records[%d].headers: %v", i, err)}
		}
		if int64(len(key)) > limits.MaxKeyBytes {
			return nil, validationError{message: fmt.Sprintf("records[%d].key exceeds configured size limit", i)}
		}

		record := producer.Record{
			Topic:     topic,
			Key:       key,
			Value:     value,
			Headers:   headers,
			Partition: rr.Partition,
		}
		if record.SizeBytes() > limits.MaxRecordBytes {
			return nil, validationError{message: fmt.Sprintf("records[%d] exceeds configured record size limit", i)}
		}

		records = append(records, record)
	}

	return records, nil
}

func decodeHeaders(headers []rawHeader, format payloadFormat, limits decodeLimits) ([]producer.Header, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	if len(headers) > limits.MaxHeaders {
		return nil, fmt.Errorf("too many headers: got %d, limit %d", len(headers), limits.MaxHeaders)
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
		if int64(len(h.Key)+len(v)) > limits.MaxHeaderBytes {
			return nil, fmt.Errorf("headers[%d] exceeds configured size limit", i)
		}
		out = append(out, producer.Header{Key: h.Key, Value: v})
	}
	return out, nil
}

func (l decodeLimits) withDefaults() decodeLimits {
	if l.MaxRecords <= 0 {
		l.MaxRecords = 1000
	}
	if l.MaxRecordBytes <= 0 {
		l.MaxRecordBytes = 1024 * 1024
	}
	if l.MaxKeyBytes <= 0 {
		l.MaxKeyBytes = 1024 * 1024
	}
	if l.MaxHeaders <= 0 {
		l.MaxHeaders = 64
	}
	if l.MaxHeaderBytes <= 0 {
		l.MaxHeaderBytes = 64 * 1024
	}
	return l
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
			return nil, binaryDecodeError{data: encoded}
		}
		return decoded, nil
	default:
		return decodeJSONValue(v.Raw)
	}
}

type binaryDecodeError struct {
	data string
}

func (e binaryDecodeError) Error() string {
	return fmt.Sprintf("Bad Request: data=%q is not a valid base64 string.", e.data)
}

func decodeJSONValue(raw json.RawMessage) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if jsonRawIsCompact(trimmed) {
		return trimmed, nil
	}
	compacted := bytes.NewBuffer(make([]byte, 0, len(trimmed)))
	if err := json.Compact(compacted, trimmed); err != nil {
		return nil, err
	}
	return compacted.Bytes(), nil
}

func jsonRawIsCompact(raw []byte) bool {
	inString := false
	escaped := false
	for _, b := range raw {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch b {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}

		switch b {
		case '"':
			inString = true
		case ' ', '\n', '\r', '\t':
			return false
		}
	}
	return true
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func responseFromResults(results []producer.Result) produceResponse {
	resp := produceResponse{Offsets: make([]produceOffset, len(results))}
	for i, r := range results {
		if resultFailed(r) {
			code := confluentKafkaRecordErrorCode
			if r.ErrorCode != nil {
				code = *r.ErrorCode
			}
			errText := "Kafka produce failed"
			if r.Err != nil {
				errText = r.Err.Error()
			}
			resp.Offsets[i] = produceOffset{
				ErrorCode: &code,
				Error:     &errText,
			}
			continue
		}

		partition := r.Partition
		offset := r.Offset
		resp.Offsets[i] = produceOffset{
			Partition: &partition,
			Offset:    &offset,
		}
	}
	return resp
}

func appendProduceResponse(dst []byte, results []producer.Result) []byte {
	if dst == nil {
		dst = make([]byte, 0, 64+(len(results)*64))
	}
	dst = append(dst, `{"offsets":[`...)
	for i, r := range results {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, '{')
		if resultFailed(r) {
			code := confluentKafkaRecordErrorCode
			if r.ErrorCode != nil {
				code = *r.ErrorCode
			}
			errText := "Kafka produce failed"
			if r.Err != nil {
				errText = r.Err.Error()
			}
			dst = append(dst, `"partition":null,"offset":null,"error_code":`...)
			dst = strconv.AppendInt(dst, int64(code), 10)
			dst = append(dst, `,"error":`...)
			dst = strconv.AppendQuote(dst, errText)
		} else {
			dst = append(dst, `"partition":`...)
			dst = strconv.AppendInt(dst, int64(r.Partition), 10)
			dst = append(dst, `,"offset":`...)
			dst = strconv.AppendInt(dst, r.Offset, 10)
			dst = append(dst, `,"error_code":null,"error":null`...)
		}
		dst = append(dst, '}')
	}
	dst = append(dst, `],"key_schema_id":null,"value_schema_id":null}`...)
	dst = append(dst, '\n')
	return dst
}

func countResultStatus(results []producer.Result) (successes, failures int) {
	for _, r := range results {
		if resultFailed(r) {
			failures++
		} else {
			successes++
		}
	}
	return successes, failures
}

func resultFailed(r producer.Result) bool {
	return r.Err != nil || r.ErrorCode != nil
}

const confluentKafkaRecordErrorCode = 50002
