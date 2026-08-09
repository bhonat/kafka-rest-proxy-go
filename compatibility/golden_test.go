package compatibility_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bhonat/kafka-rest-proxy-go/internal/api"
	"github.com/bhonat/kafka-rest-proxy-go/internal/producer"
)

type fixture struct {
	Name            string                  `json:"name"`
	Request         fixtureRequest          `json:"request"`
	ProducerResults []fixtureProducerResult `json:"producer_results"`
	Expect          fixtureExpect           `json:"expect"`
}

type fixtureRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

type fixtureProducerResult struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	ErrorCode *int   `json:"error_code"`
	Error     string `json:"error"`
}

type fixtureExpect struct {
	Status      int             `json:"status"`
	ContentType string          `json:"content_type"`
	Body        json.RawMessage `json:"body"`
}

type fixtureProducer struct {
	results []producer.Result
	records []producer.Record
}

func (p *fixtureProducer) Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error) {
	p.records = append([]producer.Record(nil), records...)
	if p.results != nil {
		return p.results, nil
	}
	out := make([]producer.Result, len(records))
	for i := range out {
		out[i] = producer.Result{Partition: int32(i), Offset: int64(i)}
	}
	return out, nil
}

func (p *fixtureProducer) Ping(ctx context.Context) error {
	return nil
}

func TestGoldenProducerCompatibilityFixtures(t *testing.T) {
	paths, err := filepath.Glob("fixtures/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no compatibility fixtures found")
	}

	for _, path := range paths {
		path := path
		t.Run(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)), func(t *testing.T) {
			fx := readFixture(t, path)
			fp := &fixtureProducer{results: fixtureResults(fx.ProducerResults)}
			h := api.NewHandler(fp, nil, api.Config{
				MaxRequestBytes: 8 * 1024 * 1024,
				MaxRecords:      1000,
				MaxRecordBytes:  1024 * 1024,
				MaxKeyBytes:     1024 * 1024,
				MaxHeaders:      64,
				MaxHeaderBytes:  64 * 1024,
				ProduceTimeout:  time.Second,
			}, nil)

			req := httptest.NewRequest(fx.Request.Method, fx.Request.Path, bytes.NewReader(fx.Request.Body))
			for k, v := range fx.Request.Headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()

			h.Routes().ServeHTTP(rec, req)

			if rec.Code != fx.Expect.Status {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, fx.Expect.Status, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); fx.Expect.ContentType != "" && got != fx.Expect.ContentType {
				t.Fatalf("Content-Type = %q, want %q", got, fx.Expect.ContentType)
			}
			assertJSONEqual(t, rec.Body.Bytes(), fx.Expect.Body)
		})
	}
}

func readFixture(t *testing.T, path string) fixture {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fx fixture
	if err := json.Unmarshal(b, &fx); err != nil {
		t.Fatal(err)
	}
	return fx
}

func fixtureResults(in []fixtureProducerResult) []producer.Result {
	if in == nil {
		return nil
	}
	out := make([]producer.Result, len(in))
	for i, r := range in {
		out[i] = producer.Result{
			Partition: r.Partition,
			Offset:    r.Offset,
			ErrorCode: r.ErrorCode,
		}
		if r.Error != "" {
			out[i].Err = errors.New(r.Error)
		}
	}
	return out
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotJSON any
	var wantJSON any
	if err := json.Unmarshal(got, &gotJSON); err != nil {
		t.Fatalf("got invalid JSON: %v\n%s", err, string(got))
	}
	if err := json.Unmarshal(want, &wantJSON); err != nil {
		t.Fatalf("want invalid JSON: %v\n%s", err, string(want))
	}
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		gotPretty, _ := json.MarshalIndent(gotJSON, "", "  ")
		wantPretty, _ := json.MarshalIndent(wantJSON, "", "  ")
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", gotPretty, wantPretty)
	}
}
