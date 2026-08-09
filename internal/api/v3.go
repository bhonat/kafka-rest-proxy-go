package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bhonat/kafka-rest-proxy-go/internal/producer"
	schemaproducer "github.com/bhonat/kafka-rest-proxy-go/internal/schema"
)

type v3ProduceHeader struct {
	Name  string  `json:"name"`
	Value *string `json:"value"`
}

type v3ProduceRecord struct {
	PartitionID *int32                      `json:"partition_id"`
	Headers     []v3ProduceHeader           `json:"headers"`
	Key         *schemaproducer.RequestData `json:"key"`
	Value       *schemaproducer.RequestData `json:"value"`
	Timestamp   *time.Time                  `json:"timestamp"`
}

type v3ProduceBatchRequest struct {
	Entries []v3ProduceBatchEntry `json:"entries"`
}

type v3ProduceBatchEntry struct {
	ID string `json:"id"`
	v3ProduceRecord
}

type v3ProduceResponse struct {
	ErrorCode   int                    `json:"error_code"`
	Message     string                 `json:"message,omitempty"`
	ClusterID   string                 `json:"cluster_id,omitempty"`
	TopicName   string                 `json:"topic_name,omitempty"`
	PartitionID *int32                 `json:"partition_id,omitempty"`
	Offset      *int64                 `json:"offset,omitempty"`
	Timestamp   *time.Time             `json:"timestamp"`
	Key         *v3ProduceResponseData `json:"key,omitempty"`
	Value       *v3ProduceResponseData `json:"value,omitempty"`
}

type v3ProduceResponseData struct {
	Size          int     `json:"size"`
	Type          string  `json:"type"`
	Subject       *string `json:"subject,omitempty"`
	SchemaID      *int    `json:"schema_id,omitempty"`
	SchemaVersion *int    `json:"schema_version,omitempty"`
}

type v3ProduceBatchResponse struct {
	Successes []v3ProduceBatchSuccess `json:"successes"`
	Failures  []v3ProduceBatchFailure `json:"failures"`
}

type v3ProduceBatchSuccess struct {
	ID          string                 `json:"id"`
	ClusterID   string                 `json:"cluster_id,omitempty"`
	TopicName   string                 `json:"topic_name,omitempty"`
	PartitionID *int32                 `json:"partition_id,omitempty"`
	Offset      *int64                 `json:"offset,omitempty"`
	Timestamp   *time.Time             `json:"timestamp"`
	Key         *v3ProduceResponseData `json:"key,omitempty"`
	Value       *v3ProduceResponseData `json:"value,omitempty"`
}

type v3ProduceBatchFailure struct {
	ID        string `json:"id"`
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message,omitempty"`
}

type v3ProduceTarget struct {
	clusterID string
	topic     string
	batch     bool
}

type v3RecordEnvelope struct {
	id     string
	record producer.Record
	key    schemaproducer.Metadata
	value  schemaproducer.Metadata
}

type v3RecordSlot struct {
	env v3RecordEnvelope
	err error
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func (h *Handler) handleV3Produce(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := http.StatusOK
	var bodyLen int64
	var recordCount int

	if h.metrics != nil {
		h.metrics.IncOutstanding()
		defer h.metrics.DecOutstanding()
		defer func() {
			h.metrics.ObserveRequest(status, bodyLen, recordCount, time.Since(start))
		}()
	}

	if r.Method != http.MethodPost {
		status = http.StatusMethodNotAllowed
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, status, errorCodeBadRequest, "method not allowed")
		return
	}
	target, ok := v3ProduceTargetFromPath(r.URL)
	if !ok {
		status = http.StatusNotFound
		writeAPIError(w, status, errorCodeBadRequest, "not found")
		return
	}
	if !h.topicAllowed(target.topic) {
		status = http.StatusForbidden
		writeAPIError(w, status, errorCodeForbidden, "topic is not allowed")
		return
	}
	if !acceptsV3Response(r) {
		status = http.StatusNotAcceptable
		writeAPIError(w, status, errorCodeNotAcceptable, "HTTP 406 Not Acceptable")
		return
	}
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		status = http.StatusUnsupportedMediaType
		writeAPIError(w, status, errorCodeUnsupportedMedia, "HTTP 415 Unsupported Media Type")
		return
	}
	if !h.allowRequestRate(1) {
		status = http.StatusTooManyRequests
		if h.metrics != nil {
			h.metrics.ObserveRateLimitRejected("requests")
		}
		writeRateLimitExceeded(w)
		return
	}

	if !target.batch {
		h.handleV3RecordStream(w, r, target, &status, &bodyLen, &recordCount)
		return
	}

	body, err := readRequestBody(w, r, h.cfg.MaxRequestBytes)
	bodyLen = int64(len(body))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			status = http.StatusRequestEntityTooLarge
			writeAPIError(w, status, http.StatusRequestEntityTooLarge, "HTTP 413 Payload Too Large")
			return
		}
		status = http.StatusBadRequest
		writeAPIError(w, status, errorCodeBadRequest, "failed to read request body")
		return
	}
	if !h.allowByteRate(bodyLen) {
		status = http.StatusTooManyRequests
		if h.metrics != nil {
			h.metrics.ObserveRateLimitRejected("bytes")
		}
		writeRateLimitExceeded(w)
		return
	}

	h.handleV3BatchBody(w, r, target, body, &status, &recordCount)
}

func (h *Handler) handleV3RecordStream(w http.ResponseWriter, r *http.Request, target v3ProduceTarget, status *int, bodyLen *int64, recordCount *int) {
	limited := http.MaxBytesReader(w, r.Body, h.cfg.MaxRequestBytes)
	counter := &countingReader{r: limited}
	dec := json.NewDecoder(counter)
	flusher, _ := w.(http.Flusher)
	wrote := false

	writeRecordResponse := func(resp v3ProduceResponse) {
		if !wrote {
			*status = http.StatusOK
			w.Header().Set("Content-Type", mediaJSON)
			w.WriteHeader(http.StatusOK)
			wrote = true
		} else {
			_, _ = w.Write([]byte("\n"))
		}
		_ = json.NewEncoder(w).Encode(resp)
		if flusher != nil {
			flusher.Flush()
		}
	}
	writeStreamError := func(code int, msg string) {
		writeRecordResponse(v3ProduceResponse{
			ErrorCode: code,
			Message:   msg,
			Timestamp: nil,
		})
	}

	for {
		beforeRead := counter.n
		var rec v3ProduceRecord
		err := dec.Decode(&rec)
		*bodyLen = counter.n
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				if wrote {
					writeStreamError(http.StatusRequestEntityTooLarge, "HTTP 413 Payload Too Large")
				} else {
					*status = http.StatusRequestEntityTooLarge
					writeAPIError(w, *status, http.StatusRequestEntityTooLarge, "HTTP 413 Payload Too Large")
				}
				return
			}
			if wrote {
				writeStreamError(errorCodeBadRequest, err.Error())
			} else {
				*status = http.StatusBadRequest
				writeAPIError(w, *status, errorCodeBadRequest, err.Error())
			}
			return
		}

		if !h.allowByteRate(counter.n - beforeRead) {
			if h.metrics != nil {
				h.metrics.ObserveRateLimitRejected("bytes")
				h.metrics.ObserveProduceResult(0, 1)
			}
			if wrote {
				writeStreamError(http.StatusTooManyRequests, rateLimitMessage)
			} else {
				*status = http.StatusTooManyRequests
				writeRateLimitExceeded(w)
			}
			return
		}

		*recordCount++
		if *recordCount > h.cfg.MaxRecords {
			msg := fmt.Sprintf("too many records: got %d, limit %d", *recordCount, h.cfg.MaxRecords)
			if h.metrics != nil {
				h.metrics.ObserveProduceResult(0, 1)
			}
			if wrote {
				writeStreamError(errorCodeBadRequest, msg)
			} else {
				*status = http.StatusBadRequest
				writeAPIError(w, *status, errorCodeBadRequest, msg)
			}
			return
		}

		env, err := h.v3EnvelopeFromRecord(r.Context(), target.topic, "", rec)
		if err != nil {
			if h.metrics != nil {
				h.metrics.ObserveProduceResult(0, 1)
			}
			writeStreamError(errorCodeBadRequest, err.Error())
			continue
		}

		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ProduceTimeout)
		produceStart := time.Now()
		results, err := h.producer.Produce(ctx, []producer.Record{env.record})
		produceWait := time.Since(produceStart)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				err = errContextCanceled
			}
			codeStatus, code, msg := classifyProduceError(err)
			if h.metrics != nil {
				h.metrics.ObserveKafkaCallbackWait(codeStatus, produceWait)
				h.metrics.ObserveProduceResult(0, 1)
				if errors.Is(err, producer.ErrOverloaded) {
					h.metrics.ObserveAdmissionRejected()
				}
			}
			if wrote {
				writeStreamError(code, msg)
			} else {
				*status = codeStatus
				writeAPIError(w, *status, code, msg)
			}
			return
		}
		if len(results) == 0 {
			results = []producer.Result{{Err: errors.New("Kafka produce returned no result")}}
		}
		if h.metrics != nil {
			h.metrics.ObserveKafkaCallbackWait(*status, produceWait)
			successes, failures := countResultStatus(results[:1])
			h.metrics.ObserveProduceResult(successes, failures)
		}
		writeRecordResponse(v3ResponseForResult(target, env, results[0]))
	}

	if *recordCount == 0 {
		*status = http.StatusBadRequest
		writeAPIError(w, *status, errorCodeBadRequest, "request body must include at least one record")
	}
}

func (h *Handler) handleV3RecordBody(w http.ResponseWriter, r *http.Request, target v3ProduceTarget, body []byte, status *int, recordCount *int) {
	slots, err := decodeV3ConcatenatedRecords(r.Context(), target.topic, body, h.schemaEncoder, h.cfg.decodeLimits())
	if err != nil {
		*status = http.StatusBadRequest
		writeAPIError(w, *status, errorCodeBadRequest, err.Error())
		return
	}
	*recordCount = len(slots)
	if len(slots) == 0 {
		*status = http.StatusBadRequest
		writeAPIError(w, *status, errorCodeBadRequest, "request body must include at least one record")
		return
	}

	responses := make([]v3ProduceResponse, len(slots))
	envelopes := make([]v3RecordEnvelope, 0, len(slots))
	responseIndexes := make([]int, 0, len(slots))
	decodeFailures := 0
	for i, slot := range slots {
		if slot.err != nil {
			decodeFailures++
			responses[i] = v3ProduceResponse{
				ErrorCode: errorCodeBadRequest,
				Message:   slot.err.Error(),
				Timestamp: nil,
			}
			continue
		}
		envelopes = append(envelopes, slot.env)
		responseIndexes = append(responseIndexes, i)
	}

	if len(envelopes) > 0 {
		producerRecords := make([]producer.Record, len(envelopes))
		for i := range envelopes {
			producerRecords[i] = envelopes[i].record
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ProduceTimeout)
		defer cancel()
		results, err := h.producer.Produce(ctx, producerRecords)
		if err != nil {
			var code int
			var msg string
			*status, code, msg = classifyProduceError(err)
			writeAPIError(w, *status, code, msg)
			return
		}
		for i, result := range results {
			responses[responseIndexes[i]] = v3ResponseForResult(target, envelopes[i], result)
		}
		if h.metrics != nil {
			successes, failures := countResultStatus(results)
			h.metrics.ObserveProduceResult(successes, failures+decodeFailures)
		}
	} else if h.metrics != nil {
		h.metrics.ObserveProduceResult(0, decodeFailures)
	}

	w.Header().Set("Content-Type", mediaJSON)
	w.WriteHeader(http.StatusOK)
	for i, resp := range responses {
		if i > 0 {
			_, _ = w.Write([]byte("\n"))
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func (h *Handler) handleV3BatchBody(w http.ResponseWriter, r *http.Request, target v3ProduceTarget, body []byte, status *int, recordCount *int) {
	var req v3ProduceBatchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		*status = http.StatusBadRequest
		writeAPIError(w, *status, errorCodeBadRequest, err.Error())
		return
	}
	if len(req.Entries) == 0 {
		*status = http.StatusBadRequest
		writeAPIError(w, *status, errorCodeBadRequest, "entries must not be empty")
		return
	}
	if len(req.Entries) > h.cfg.MaxRecords {
		*status = http.StatusBadRequest
		writeAPIError(w, *status, errorCodeBadRequest, fmt.Sprintf("too many entries: got %d, limit %d", len(req.Entries), h.cfg.MaxRecords))
		return
	}

	seen := map[string]struct{}{}
	envelopes := make([]v3RecordEnvelope, 0, len(req.Entries))
	response := v3ProduceBatchResponse{Successes: []v3ProduceBatchSuccess{}, Failures: []v3ProduceBatchFailure{}}
	for i := range req.Entries {
		entry := req.Entries[i]
		if err := validateBatchID(entry.ID, seen); err != nil {
			response.Failures = append(response.Failures, v3ProduceBatchFailure{ID: entry.ID, ErrorCode: errorCodeBadRequest, Message: err.Error()})
			continue
		}
		env, err := h.v3EnvelopeFromRecord(r.Context(), target.topic, entry.ID, entry.v3ProduceRecord)
		if err != nil {
			response.Failures = append(response.Failures, v3ProduceBatchFailure{ID: entry.ID, ErrorCode: errorCodeBadRequest, Message: err.Error()})
			continue
		}
		envelopes = append(envelopes, env)
	}
	*recordCount = len(envelopes)
	if len(envelopes) > 0 {
		producerRecords := make([]producer.Record, len(envelopes))
		for i := range envelopes {
			producerRecords[i] = envelopes[i].record
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ProduceTimeout)
		defer cancel()
		results, err := h.producer.Produce(ctx, producerRecords)
		if err != nil {
			var code int
			var msg string
			*status, code, msg = classifyProduceError(err)
			writeAPIError(w, *status, code, msg)
			return
		}
		if h.metrics != nil {
			successes, failures := countResultStatus(results)
			h.metrics.ObserveProduceResult(successes, failures)
		}
		for i, result := range results {
			if resultFailed(result) {
				response.Failures = append(response.Failures, v3ProduceBatchFailure{
					ID:        envelopes[i].id,
					ErrorCode: resultErrorCode(result),
					Message:   resultErrorMessage(result),
				})
				continue
			}
			response.Successes = append(response.Successes, v3BatchSuccessForResult(target, envelopes[i], result))
		}
	}

	*status = http.StatusMultiStatus
	w.Header().Set("Content-Type", mediaJSON)
	w.WriteHeader(*status)
	_ = json.NewEncoder(w).Encode(response)
}

func decodeV3ConcatenatedRecords(ctx context.Context, topic string, body []byte, enc *schemaproducer.Encoder, limits decodeLimits) ([]v3RecordSlot, error) {
	limits = limits.withDefaults()
	dec := json.NewDecoder(bytes.NewReader(body))
	var records []v3RecordSlot
	for {
		var rec v3ProduceRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		env, err := v3EnvelopeFromRecord(ctx, topic, "", rec, enc, limits)
		if err != nil {
			records = append(records, v3RecordSlot{err: err})
		} else {
			records = append(records, v3RecordSlot{env: env})
		}
		if len(records) > limits.MaxRecords {
			return nil, fmt.Errorf("too many records: got %d, limit %d", len(records), limits.MaxRecords)
		}
	}
	return records, nil
}

func (h *Handler) v3EnvelopeFromRecord(ctx context.Context, topic string, id string, rec v3ProduceRecord) (v3RecordEnvelope, error) {
	return v3EnvelopeFromRecord(ctx, topic, id, rec, h.schemaEncoder, h.cfg.decodeLimits())
}

func v3EnvelopeFromRecord(ctx context.Context, topic string, id string, rec v3ProduceRecord, enc *schemaproducer.Encoder, limits decodeLimits) (v3RecordEnvelope, error) {
	key, keyMeta, err := encodeV3Data(ctx, topic, true, rec.Key, enc)
	if err != nil {
		return v3RecordEnvelope{}, fmt.Errorf("key: %w", err)
	}
	value, valueMeta, err := encodeV3Data(ctx, topic, false, rec.Value, enc)
	if err != nil {
		return v3RecordEnvelope{}, fmt.Errorf("value: %w", err)
	}
	if int64(len(key)) > limits.MaxKeyBytes {
		return v3RecordEnvelope{}, fmt.Errorf("key exceeds configured size limit")
	}
	if rec.PartitionID != nil && *rec.PartitionID < 0 {
		return v3RecordEnvelope{}, fmt.Errorf("partition_id must be non-negative")
	}
	headers, err := decodeV3Headers(rec.Headers, limits)
	if err != nil {
		return v3RecordEnvelope{}, err
	}
	record := producer.Record{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Headers:   headers,
		Partition: rec.PartitionID,
	}
	if record.SizeBytes() > limits.MaxRecordBytes {
		return v3RecordEnvelope{}, fmt.Errorf("record exceeds configured record size limit")
	}
	return v3RecordEnvelope{id: id, record: record, key: keyMeta, value: valueMeta}, nil
}

func encodeV3Data(ctx context.Context, topic string, key bool, data *schemaproducer.RequestData, enc *schemaproducer.Encoder) ([]byte, schemaproducer.Metadata, error) {
	if data == nil {
		return nil, schemaproducer.Metadata{}, nil
	}
	if data.IsSchemaAware() && enc == nil {
		return nil, schemaproducer.Metadata{}, fmt.Errorf("schema registry is required")
	}
	if enc == nil {
		enc = schemaproducer.NewEncoder(nil)
	}
	return enc.Encode(ctx, topic, key, data)
}

func decodeV3Headers(headers []v3ProduceHeader, limits decodeLimits) ([]producer.Header, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	if len(headers) > limits.MaxHeaders {
		return nil, fmt.Errorf("too many headers: got %d, limit %d", len(headers), limits.MaxHeaders)
	}
	out := make([]producer.Header, 0, len(headers))
	for i, h := range headers {
		if h.Name == "" {
			return nil, fmt.Errorf("headers[%d].name is required", i)
		}
		var value []byte
		if h.Value != nil {
			decoded, err := base64.StdEncoding.DecodeString(*h.Value)
			if err != nil {
				return nil, fmt.Errorf("headers[%d].value is not valid base64", i)
			}
			value = decoded
		}
		if int64(len(h.Name)+len(value)) > limits.MaxHeaderBytes {
			return nil, fmt.Errorf("headers[%d] exceeds configured size limit", i)
		}
		out = append(out, producer.Header{Key: h.Name, Value: value})
	}
	return out, nil
}

func v3ProduceTargetFromPath(u *url.URL) (v3ProduceTarget, bool) {
	segments := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if len(segments) != 6 {
		return v3ProduceTarget{}, false
	}
	if segments[0] != "v3" || segments[1] != "clusters" || segments[3] != "topics" {
		return v3ProduceTarget{}, false
	}
	clusterID, err := url.PathUnescape(segments[2])
	if err != nil || clusterID == "" {
		return v3ProduceTarget{}, false
	}
	topic, err := url.PathUnescape(segments[4])
	if err != nil || topic == "" {
		return v3ProduceTarget{}, false
	}
	switch segments[5] {
	case "records":
		return v3ProduceTarget{clusterID: clusterID, topic: topic}, true
	case "records:batch":
		return v3ProduceTarget{clusterID: clusterID, topic: topic, batch: true}, true
	default:
		return v3ProduceTarget{}, false
	}
}

func v3ResponseForResult(target v3ProduceTarget, env v3RecordEnvelope, result producer.Result) v3ProduceResponse {
	if resultFailed(result) {
		return v3ProduceResponse{
			ErrorCode: resultErrorCode(result),
			Message:   resultErrorMessage(result),
			Timestamp: nil,
		}
	}
	partition := result.Partition
	offset := result.Offset
	timestamp := time.Now().UTC()
	return v3ProduceResponse{
		ErrorCode:   http.StatusOK,
		ClusterID:   firstNonEmpty(target.clusterID, "local"),
		TopicName:   target.topic,
		PartitionID: &partition,
		Offset:      &offset,
		Timestamp:   &timestamp,
		Key:         v3ResponseData(env.key),
		Value:       v3ResponseData(env.value),
	}
}

func v3BatchSuccessForResult(target v3ProduceTarget, env v3RecordEnvelope, result producer.Result) v3ProduceBatchSuccess {
	resp := v3ResponseForResult(target, env, result)
	return v3ProduceBatchSuccess{
		ID:          env.id,
		ClusterID:   resp.ClusterID,
		TopicName:   resp.TopicName,
		PartitionID: resp.PartitionID,
		Offset:      resp.Offset,
		Timestamp:   resp.Timestamp,
		Key:         resp.Key,
		Value:       resp.Value,
	}
}

func v3ResponseData(meta schemaproducer.Metadata) *v3ProduceResponseData {
	if meta.Type == "" && meta.Size == 0 && meta.SchemaID == nil {
		return nil
	}
	resp := &v3ProduceResponseData{
		Size:          meta.Size,
		Type:          meta.Type,
		SchemaID:      meta.SchemaID,
		SchemaVersion: meta.SchemaVersion,
	}
	if meta.Subject != "" {
		subject := meta.Subject
		resp.Subject = &subject
	}
	return resp
}

func resultErrorCode(result producer.Result) int {
	if result.ErrorCode != nil {
		return *result.ErrorCode
	}
	return confluentKafkaRecordErrorCode
}

func resultErrorMessage(result producer.Result) string {
	if result.Err != nil {
		return result.Err.Error()
	}
	return "Kafka produce failed"
}

func validateBatchID(id string, seen map[string]struct{}) error {
	if id == "" {
		return fmt.Errorf("id is required")
	}
	if len(id) > 80 {
		return fmt.Errorf("id exceeds 80 characters")
	}
	if _, err := strconv.Atoi(id); err == nil {
		// Numeric strings are valid strings. This branch intentionally only
		// exercises strconv so future validation changes keep numeric IDs in mind.
	}
	if _, ok := seen[id]; ok {
		return fmt.Errorf("duplicate id %q", id)
	}
	seen[id] = struct{}{}
	return nil
}

func isJSONContentType(header string) bool {
	if strings.TrimSpace(header) == "" {
		return true
	}
	return strings.Contains(strings.ToLower(header), "application/json")
}

func acceptsV3Response(r *http.Request) bool {
	accept := strings.TrimSpace(r.Header.Get("Accept"))
	return accept == "" || strings.Contains(accept, "*/*") || strings.Contains(strings.ToLower(accept), "application/json")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
