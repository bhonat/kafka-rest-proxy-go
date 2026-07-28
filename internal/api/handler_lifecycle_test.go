package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

type blockingLifecycleProducer struct {
	started    chan struct{}
	cancelSeen chan struct{}
	startOnce  sync.Once
	cancelOnce sync.Once
}

func newBlockingLifecycleProducer() *blockingLifecycleProducer {
	return &blockingLifecycleProducer{
		started:    make(chan struct{}),
		cancelSeen: make(chan struct{}),
	}
}

func (p *blockingLifecycleProducer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	p.startOnce.Do(func() { close(p.started) })
	<-ctx.Done()
	p.cancelOnce.Do(func() { close(p.cancelSeen) })
	return nil, ctx.Err()
}

func (p *blockingLifecycleProducer) Ping(ctx context.Context) error {
	return nil
}

type countingLifecycleProducer struct {
	calls atomic.Int64
}

func (p *countingLifecycleProducer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	call := p.calls.Add(1)
	results := make([]producer.Result, len(records))
	for i := range results {
		results[i] = producer.Result{Partition: int32(i % 7), Offset: call*1000 + int64(i)}
	}
	return results, nil
}

func (p *countingLifecycleProducer) Ping(ctx context.Context) error {
	return nil
}

func TestProduceTimeoutCancelsProducerAndReturnsHandler(t *testing.T) {
	fp := newBlockingLifecycleProducer()
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  20 * time.Millisecond,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		h.Routes().ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-fp.started:
	case <-time.After(time.Second):
		t.Fatal("producer was not called")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler goroutine did not return after produce timeout")
	}
	select {
	case <-fp.cancelSeen:
	default:
		t.Fatal("producer did not observe context cancellation")
	}
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRepeatedProduceRequestsDoNotShareResponseState(t *testing.T) {
	fp := &countingLifecycleProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)
	routes := h.Routes()

	const requests = 250
	for i := 0; i < requests; i++ {
		body := fmt.Sprintf(`{"records":[{"value":{"i":%d}},{"value":null}]}`, i)
		req := httptest.NewRequest(http.MethodPost, "/topics/orders", strings.NewReader(body))
		req.Header.Set("Content-Type", mediaKafkaJSONV2)
		rec := httptest.NewRecorder()

		routes.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d body=%s", i, rec.Code, rec.Body.String())
		}
		var resp produceResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("request %d response JSON: %v", i, err)
		}
		if len(resp.Offsets) != 2 {
			t.Fatalf("request %d offsets = %d, want 2", i, len(resp.Offsets))
		}
		if resp.Offsets[0].Offset == nil || resp.Offsets[1].Offset == nil {
			t.Fatalf("request %d missing offsets: %#v", i, resp.Offsets)
		}
		if *resp.Offsets[1].Offset != *resp.Offsets[0].Offset+1 {
			t.Fatalf("request %d offsets not request-local: %#v", i, resp.Offsets)
		}
	}

	if got := fp.calls.Load(); got != requests {
		t.Fatalf("producer calls = %d, want %d", got, requests)
	}
}

func TestPprofRouteRegistrationDoesNotLeakAcrossHandlers(t *testing.T) {
	enabled := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
		PprofEnable:     true,
	}, nil).Routes()
	disabled := NewHandler(&fakeProducer{}, nil, Config{
		MaxRequestBytes: 1024 * 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
		PprofEnable:     false,
	}, nil).Routes()

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
		rec := httptest.NewRecorder()
		enabled.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("enabled request %d status = %d body=%s", i, rec.Code, rec.Body.String())
		}

		req = httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
		rec = httptest.NewRecorder()
		disabled.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("disabled request %d status = %d body=%s", i, rec.Code, rec.Body.String())
		}
	}
}
