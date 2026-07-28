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

	"github.com/example/kafka-rest-proxy-go/internal/metrics"
	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

type securityProducer struct {
	produceCalls int
	pingCalls    int
	pingErr      error
	records      []producer.Record
}

func (p *securityProducer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	p.produceCalls++
	p.records = append([]producer.Record(nil), records...)

	results := make([]producer.Result, len(records))
	for i := range results {
		results[i] = producer.Result{
			Partition: int32(i),
			Offset:    int64(1000 + i),
		}
	}
	return results, nil
}

func (p *securityProducer) Ping(ctx context.Context) error {
	p.pingCalls++
	return p.pingErr
}

func TestSecurityBearerAuthRequiredForProduce(t *testing.T) {
	prod := &securityProducer{}
	h := NewHandler(prod, nil, securityTestConfig(Config{
		BearerTokens: []string{"secret"},
	}), nil).Routes()

	tests := []struct {
		name        string
		auth        string
		wantStatus  int
		wantMessage string
	}{
		{
			name:        "missing header",
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "missing bearer token",
		},
		{
			name:        "basic auth is not accepted",
			auth:        "Basic secret",
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "missing bearer token",
		},
		{
			name:        "bearer without token",
			auth:        "Bearer",
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "missing bearer token",
		},
		{
			name:        "lowercase bearer scheme is not accepted",
			auth:        "bearer secret",
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "missing bearer token",
		},
		{
			name:        "wrong token",
			auth:        "Bearer wrong",
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "invalid bearer token",
		},
		{
			name:        "extra token material",
			auth:        "Bearer secret extra",
			wantStatus:  http.StatusUnauthorized,
			wantMessage: "invalid bearer token",
		},
		{
			name:       "valid token",
			auth:       "Bearer secret",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := prod.produceCalls
			rec := serveSecurityProduce(h, "/topics/orders", tt.auth)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				if prod.produceCalls != before+1 {
					t.Fatalf("produce calls = %d, want %d", prod.produceCalls, before+1)
				}
				if len(prod.records) != 1 || prod.records[0].Topic != "orders" {
					t.Fatalf("produced records = %#v, want one orders record", prod.records)
				}
				return
			}

			if prod.produceCalls != before {
				t.Fatalf("produce was called for rejected request")
			}
			resp := decodeSecurityError(t, rec)
			if resp.ErrorCode != errorCodeUnauthorized || resp.Message != tt.wantMessage {
				t.Fatalf("error response = %#v", resp)
			}
		})
	}
}

func TestSecurityHealthAndReadyBypassBearerAuth(t *testing.T) {
	prod := &securityProducer{}
	h := NewHandler(prod, nil, securityTestConfig(Config{
		BearerTokens: []string{"secret"},
	}), nil).Routes()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d body=%s", rec.Code, rec.Body.String())
	}
	if prod.pingCalls != 0 {
		t.Fatalf("health should not ping Kafka, ping calls = %d", prod.pingCalls)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d body=%s", rec.Code, rec.Body.String())
	}
	if prod.pingCalls != 1 {
		t.Fatalf("ready ping calls = %d, want 1", prod.pingCalls)
	}

	unhealthy := &securityProducer{pingErr: errors.New("kafka unavailable")}
	h = NewHandler(unhealthy, nil, securityTestConfig(Config{
		BearerTokens: []string{"secret"},
	}), nil).Routes()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy ready status = %d body=%s", rec.Code, rec.Body.String())
	}
	if unhealthy.pingCalls != 1 {
		t.Fatalf("unhealthy ready ping calls = %d, want 1", unhealthy.pingCalls)
	}
}

func TestSecurityTopicAllowlistExactPrefixAndDeniedTopics(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantTopic  string
	}{
		{
			name:       "exact topic is allowed",
			path:       "/topics/orders",
			wantStatus: http.StatusOK,
			wantTopic:  "orders",
		},
		{
			name:       "prefix topic is allowed",
			path:       "/topics/tenant-alpha",
			wantStatus: http.StatusOK,
			wantTopic:  "tenant-alpha",
		},
		{
			name:       "exact topic does not imply prefix",
			path:       "/topics/orders-v2",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "unlisted topic is denied",
			path:       "/topics/payments",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "near prefix is denied",
			path:       "/topics/tenantless-alpha",
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prod := &securityProducer{}
			h := NewHandler(prod, nil, securityTestConfig(Config{
				AllowedTopics: []string{"orders", "tenant-*"},
			}), nil).Routes()

			rec := serveSecurityProduce(h, tt.path, "")
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}

			if tt.wantStatus == http.StatusOK {
				if prod.produceCalls != 1 {
					t.Fatalf("produce calls = %d, want 1", prod.produceCalls)
				}
				if len(prod.records) != 1 || prod.records[0].Topic != tt.wantTopic {
					t.Fatalf("produced records = %#v, want topic %q", prod.records, tt.wantTopic)
				}
				return
			}

			if prod.produceCalls != 0 {
				t.Fatalf("produce was called for denied topic")
			}
			resp := decodeSecurityError(t, rec)
			if resp.ErrorCode != errorCodeForbidden || resp.Message != "topic is not allowed" {
				t.Fatalf("error response = %#v", resp)
			}
		})
	}
}

func TestSecurityAuthRunsBeforeTopicAllowlist(t *testing.T) {
	prod := &securityProducer{}
	h := NewHandler(prod, nil, securityTestConfig(Config{
		BearerTokens:  []string{"secret"},
		AllowedTopics: []string{"orders"},
	}), nil).Routes()

	rec := serveSecurityProduce(h, "/topics/payments", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated denied topic status = %d body=%s", rec.Code, rec.Body.String())
	}
	if prod.produceCalls != 0 {
		t.Fatalf("produce was called for unauthenticated denied topic")
	}

	rec = serveSecurityProduce(h, "/topics/payments", "Bearer secret")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("authenticated denied topic status = %d body=%s", rec.Code, rec.Body.String())
	}
	if prod.produceCalls != 0 {
		t.Fatalf("produce was called for authenticated denied topic")
	}
}

func TestSecurityMetricsExposureIsIntentional(t *testing.T) {
	t.Run("metrics mounted without auth when metrics are configured", func(t *testing.T) {
		m := newSecurityMetrics(t)
		h := NewHandler(&securityProducer{}, m, securityTestConfig(Config{}), nil).Routes()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("metrics status = %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("metrics require bearer token when auth is configured", func(t *testing.T) {
		m := newSecurityMetrics(t)
		h := NewHandler(&securityProducer{}, m, securityTestConfig(Config{
			BearerTokens: []string{"secret"},
		}), nil).Routes()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated metrics status = %d body=%s", rec.Code, rec.Body.String())
		}

		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("authenticated metrics status = %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("metrics are not mounted when metrics are nil", func(t *testing.T) {
		h := NewHandler(&securityProducer{}, nil, securityTestConfig(Config{}), nil).Routes()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

		if rec.Code != http.StatusNotFound {
			t.Fatalf("metrics status = %d, want 404", rec.Code)
		}
	})
}

func TestSecurityPprofExposureIsIntentional(t *testing.T) {
	t.Run("pprof disabled without auth returns not found", func(t *testing.T) {
		h := NewHandler(&securityProducer{}, nil, securityTestConfig(Config{}), nil).Routes()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

		if rec.Code != http.StatusNotFound {
			t.Fatalf("pprof status = %d, want 404", rec.Code)
		}
	})

	t.Run("pprof disabled with auth returns unauthorized before route disclosure", func(t *testing.T) {
		h := NewHandler(&securityProducer{}, nil, securityTestConfig(Config{
			BearerTokens: []string{"secret"},
		}), nil).Routes()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated pprof status = %d body=%s", rec.Code, rec.Body.String())
		}

		req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("authenticated disabled pprof status = %d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("pprof enabled requires bearer token when auth is configured", func(t *testing.T) {
		h := NewHandler(&securityProducer{}, nil, securityTestConfig(Config{
			BearerTokens: []string{"secret"},
			PprofEnable:  true,
		}), nil).Routes()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated pprof status = %d body=%s", rec.Code, rec.Body.String())
		}

		req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("authenticated pprof status = %d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func serveSecurityProduce(h http.Handler, path string, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func securityTestConfig(cfg Config) Config {
	cfg.MaxRequestBytes = 1024 * 1024
	cfg.MaxRecords = 10
	cfg.ProduceTimeout = time.Second
	return cfg
}

func decodeSecurityError(t *testing.T, rec *httptest.ResponseRecorder) errorResponse {
	t.Helper()

	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, rec.Body.String())
	}
	return resp
}

func newSecurityMetrics(t *testing.T) *metrics.Metrics {
	t.Helper()

	m, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Fatalf("shutdown metrics: %v", err)
		}
	})
	return m
}
