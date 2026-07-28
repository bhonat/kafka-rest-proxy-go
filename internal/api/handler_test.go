package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

type fakeProducer struct {
	records     []producer.Record
	results     []producer.Result
	err         error
	produceFunc func(context.Context, []producer.Record) ([]producer.Result, error)
}

func (f *fakeProducer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	f.records = append([]producer.Record(nil), records...)
	if f.produceFunc != nil {
		return f.produceFunc(ctx, records)
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.results != nil {
		return f.results, nil
	}
	out := make([]producer.Result, len(records))
	for i := range out {
		out[i] = producer.Result{Partition: int32(i), Offset: int64(100 + i)}
	}
	return out, nil
}

func (f *fakeProducer) Ping(ctx context.Context) error {
	return nil
}

func TestJSONProduce(t *testing.T) {
	fp := &fakeProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	body := `{"records":[{"key":"customer-1","value":{"order_id":"o-1"},"partition":2}]}`
	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(fp.records) != 1 {
		t.Fatalf("records = %d, want 1", len(fp.records))
	}
	if fp.records[0].Topic != "orders" {
		t.Fatalf("topic = %q", fp.records[0].Topic)
	}
	if string(fp.records[0].Key) != `"customer-1"` {
		t.Fatalf("key bytes = %q", string(fp.records[0].Key))
	}
	if string(fp.records[0].Value) != `{"order_id":"o-1"}` {
		t.Fatalf("value bytes = %q", string(fp.records[0].Value))
	}
	if fp.records[0].Partition == nil || *fp.records[0].Partition != 2 {
		t.Fatalf("partition = %v", fp.records[0].Partition)
	}

	var resp produceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.KeySchemaID != nil || resp.ValueSchemaID != nil {
		t.Fatalf("schema ids = %#v/%#v, want null/null", resp.KeySchemaID, resp.ValueSchemaID)
	}
	if len(resp.Offsets) != 1 || resp.Offsets[0].Offset == nil || *resp.Offsets[0].Offset != 100 {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestKafkaRecordErrorsReturnHTTP200WithPerRecordErrors(t *testing.T) {
	code := 50002
	fp := &fakeProducer{
		results: []producer.Result{
			{Partition: 2, Offset: 100},
			{ErrorCode: &code, Err: errors.New("Invalid topics: [bad topic]")},
		},
	}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	body := `{"records":[{"value":{"ok":true}},{"value":{"ok":false}}]}`
	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}

	var resp produceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Offsets) != 2 {
		t.Fatalf("offsets = %d, want 2", len(resp.Offsets))
	}
	if resp.Offsets[0].Partition == nil || *resp.Offsets[0].Partition != 2 {
		t.Fatalf("success partition = %#v", resp.Offsets[0].Partition)
	}
	if resp.Offsets[0].Offset == nil || *resp.Offsets[0].Offset != 100 {
		t.Fatalf("success offset = %#v", resp.Offsets[0].Offset)
	}
	if resp.Offsets[0].ErrorCode != nil || resp.Offsets[0].Error != nil {
		t.Fatalf("success should not include error: %#v", resp.Offsets[0])
	}
	if resp.Offsets[1].Partition != nil || resp.Offsets[1].Offset != nil {
		t.Fatalf("failed record should have null partition/offset: %#v", resp.Offsets[1])
	}
	if resp.Offsets[1].ErrorCode == nil || *resp.Offsets[1].ErrorCode != 50002 {
		t.Fatalf("error_code = %#v, want 50002", resp.Offsets[1].ErrorCode)
	}
	if resp.Offsets[1].Error == nil || *resp.Offsets[1].Error != "Invalid topics: [bad topic]" {
		t.Fatalf("error = %#v", resp.Offsets[1].Error)
	}
}

func TestKafkaRecordErrorDefaultsToConfluentCode(t *testing.T) {
	fp := &fakeProducer{
		results: []producer.Result{
			{Err: errors.New("The message is larger than max.request.size")},
		},
	}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp produceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Offsets[0].ErrorCode == nil || *resp.Offsets[0].ErrorCode != 50002 {
		t.Fatalf("error_code = %#v, want 50002", resp.Offsets[0].ErrorCode)
	}
}

func TestProducerOverloadReturnsTooManyRequests(t *testing.T) {
	fp := &fakeProducer{err: producer.ErrOverloaded}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 429 || resp.Message != "producer is overloaded" {
		t.Fatalf("unexpected error response: %#v", resp)
	}
	if len(fp.records) != 1 {
		t.Fatalf("producer records = %d, want 1", len(fp.records))
	}
}

func TestProduceTimeoutReturnsGatewayTimeout(t *testing.T) {
	fp := &fakeProducer{
		produceFunc: func(ctx context.Context, _ []producer.Record) ([]producer.Result, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Nanosecond,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 504 || resp.Message != "produce request timed out" {
		t.Fatalf("unexpected error response: %#v", resp)
	}
}

func TestCanceledProduceContextReturnsGatewayTimeout(t *testing.T) {
	fp := &fakeProducer{
		produceFunc: func(ctx context.Context, _ []producer.Record) ([]producer.Result, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 504 || resp.Message != "produce request timed out" {
		t.Fatalf("unexpected error response: %#v", resp)
	}
}

func TestKafkaSideInvalidTopicAndPartitionReturnRecordErrors(t *testing.T) {
	code := 50002
	partition := int32(99)
	tests := []struct {
		name          string
		path          string
		body          string
		wantTopic     string
		wantPartition *int32
		errMessage    string
	}{
		{
			name:       "invalid_topic_from_kafka",
			path:       "/topics/bad%20topic",
			body:       `{"records":[{"value":{"ok":true}}]}`,
			wantTopic:  "bad topic",
			errMessage: "Invalid topics: [bad topic]",
		},
		{
			name:          "invalid_partition_from_kafka",
			path:          "/topics/orders",
			body:          `{"records":[{"value":{"ok":true},"partition":99}]}`,
			wantTopic:     "orders",
			wantPartition: &partition,
			errMessage:    "This server does not host this topic-partition",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakeProducer{
				produceFunc: func(_ context.Context, records []producer.Record) ([]producer.Result, error) {
					if len(records) != 1 {
						t.Fatalf("records = %d, want 1", len(records))
					}
					if records[0].Topic != tc.wantTopic {
						t.Fatalf("topic = %q, want %q", records[0].Topic, tc.wantTopic)
					}
					if tc.wantPartition == nil {
						if records[0].Partition != nil {
							t.Fatalf("partition = %v, want nil", *records[0].Partition)
						}
					} else if records[0].Partition == nil || *records[0].Partition != *tc.wantPartition {
						t.Fatalf("partition = %v, want %v", records[0].Partition, *tc.wantPartition)
					}
					return []producer.Result{
						{ErrorCode: &code, Err: errors.New(tc.errMessage)},
					}, nil
				},
			}
			h := NewHandler(fp, nil, Config{
				MaxRequestBytes: 1024 * 1024,
				MaxRecords:      10,
				ProduceTimeout:  time.Second,
			}, nil)

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", mediaKafkaJSONV2)
			req.Header.Set("Accept", mediaKafkaV2)
			rec := httptest.NewRecorder()

			h.Routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
			var resp produceResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if len(resp.Offsets) != 1 {
				t.Fatalf("offsets = %d, want 1", len(resp.Offsets))
			}
			got := resp.Offsets[0]
			if got.Partition != nil || got.Offset != nil {
				t.Fatalf("failed record should have null partition/offset: %#v", got)
			}
			if got.ErrorCode == nil || *got.ErrorCode != code {
				t.Fatalf("error_code = %#v, want %d", got.ErrorCode, code)
			}
			if got.Error == nil || *got.Error != tc.errMessage {
				t.Fatalf("error = %#v, want %q", got.Error, tc.errMessage)
			}
		})
	}
}

func TestBinaryProduce(t *testing.T) {
	fp := &fakeProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	body := `{"records":[{"key":"a2V5","value":"dmFsdWU="}]}`
	req := httptest.NewRequest(http.MethodPost, "/topics/bin", strings.NewReader(body))
	req.Header.Set("Content-Type", mediaKafkaBinaryV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if string(fp.records[0].Key) != "key" {
		t.Fatalf("key bytes = %q", string(fp.records[0].Key))
	}
	if string(fp.records[0].Value) != "value" {
		t.Fatalf("value bytes = %q", string(fp.records[0].Value))
	}
}

func TestRejectUnsupportedContentType(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[]}`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Message != "HTTP 415 Unsupported Media Type" {
		t.Fatalf("message = %q", resp.Message)
	}
}

func TestRejectUnsupportedAcceptConfluentShape(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", "application/xml")
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 406 || resp.Message != "HTTP 406 Not Acceptable" {
		t.Fatalf("unexpected error response: %#v", resp)
	}
}

func TestMalformedJSONReturnsBadRequest(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 400 {
		t.Fatalf("error_code = %d, want 400", resp.ErrorCode)
	}
}

func TestBinaryDecodeFailureReturnsConfluentShape(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":"not-base64!"}]}`))
	req.Header.Set("Content-Type", mediaKafkaBinaryV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	want := `Bad Request: data="not-base64!" is not a valid base64 string.`
	if resp.ErrorCode != 400 || resp.Message != want {
		t.Fatalf("unexpected error response: %#v", resp)
	}
}

func TestBearerAuth(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
		BearerTokens:    []string{"secret"},
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec = httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("produce status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestTopicAllowlist(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
		AllowedTopics:   []string{"orders", "integration-*"},
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/payments", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/topics/integration-123", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	rec = httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("wildcard status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPprofDisabledByDefault(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestPprofEnabled(t *testing.T) {
	h := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
		PprofEnable:     true,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}
