package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	appmetrics "github.com/bhonat/kafka-rest-proxy-go/internal/metrics"
	"github.com/bhonat/kafka-rest-proxy-go/internal/producer"
)

func TestBackpressureOverloadedProducerReturnsTooManyRequests(t *testing.T) {
	h := NewHandler(&fakeProducer{err: producer.ErrOverloaded}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	rec := serveBackpressureProduce(h, `{"records":[{"value":{"ok":true}}]}`)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d body=%s, want 429", rec.Code, rec.Body.String())
	}
	assertBackpressureError(t, rec, errorCodeTooManyRequests, "producer is overloaded")
}

func TestBackpressureGenericProducerErrorReturnsServiceUnavailable(t *testing.T) {
	h := NewHandler(&fakeProducer{err: errors.New("metadata unavailable")}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	rec := serveBackpressureProduce(h, `{"records":[{"value":{"ok":true}}]}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s, want 503", rec.Code, rec.Body.String())
	}
	assertBackpressureError(t, rec, errorCodeProduceUnavailable, "metadata unavailable")
}

func TestBackpressureContextCancellationReturnsGatewayTimeout(t *testing.T) {
	h := NewHandler(blockingProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Nanosecond,
	}, nil)

	rec := serveBackpressureProduce(h, `{"records":[{"value":{"ok":true}}]}`)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d body=%s, want 504", rec.Code, rec.Body.String())
	}
	assertBackpressureError(t, rec, errorCodeTimeout, "produce request timed out")
}

func TestBackpressureOverloadedProducerIncrementsAdmissionRejectionMetric(t *testing.T) {
	m, err := appmetrics.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown metrics: %v", err)
		}
	}()

	h := NewHandler(&fakeProducer{err: producer.ErrOverloaded}, m, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	produceRec := serveBackpressureProduce(h, `{"records":[{"value":{"ok":true}}]}`)
	if produceRec.Code != http.StatusTooManyRequests {
		t.Fatalf("produce status = %d body=%s, want 429", produceRec.Code, produceRec.Body.String())
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	h.Routes().ServeHTTP(metricsRec, metricsReq)
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d body=%s", metricsRec.Code, metricsRec.Body.String())
	}
	got := metricsRec.Body.String()
	assertBackpressureMetric(t, got, `kafka_rest_admission_rejections_total`, ``, "1")
	assertBackpressureMetric(t, got, `kafka_rest_requests_total`, `status_class="4xx"`, "1")
	assertBackpressureMetricAbsent(t, got, `kafka_rest_requests_total`, `status_class="2xx"`)
}

type blockingProducer struct{}

func (blockingProducer) Produce(ctx context.Context, _ []producer.Record) ([]producer.Result, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingProducer) Ping(context.Context) error {
	return nil
}

func serveBackpressureProduce(h *Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func assertBackpressureError(t *testing.T, rec *httptest.ResponseRecorder, wantCode int, wantMessage string) {
	t.Helper()

	if got := rec.Header().Get("Content-Type"); got != mediaKafkaV2 {
		t.Fatalf("content-type = %q, want %q", got, mediaKafkaV2)
	}

	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != wantCode || resp.Message != wantMessage {
		t.Fatalf("error response = %#v, want code=%d message=%q", resp, wantCode, wantMessage)
	}
}

func assertBackpressureMetric(t *testing.T, body, name, labelFragment, value string) {
	t.Helper()

	pattern := `(?m)^` + regexp.QuoteMeta(name) + `\{[^\n]*` + regexp.QuoteMeta(labelFragment) + `[^\n]*\} ` + regexp.QuoteMeta(value) + `$`
	if !regexp.MustCompile(pattern).MatchString(body) {
		t.Fatalf("metrics did not include %s{%s} %s:\n%s", name, labelFragment, value, body)
	}
}

func assertBackpressureMetricAbsent(t *testing.T, body, name, labelFragment string) {
	t.Helper()

	pattern := `(?m)^` + regexp.QuoteMeta(name) + `\{[^\n]*` + regexp.QuoteMeta(labelFragment) + `[^\n]*\} `
	if regexp.MustCompile(pattern).MatchString(body) {
		t.Fatalf("metrics unexpectedly included %s{%s}:\n%s", name, labelFragment, body)
	}
}
