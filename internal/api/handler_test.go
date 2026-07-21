package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

type fakeProducer struct {
	records []producer.Record
	results []producer.Result
	err     error
}

func (f *fakeProducer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	f.records = append([]producer.Record(nil), records...)
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
	if len(resp.Offsets) != 1 || resp.Offsets[0].Offset != 100 {
		t.Fatalf("unexpected response: %#v", resp)
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
