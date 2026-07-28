package api

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestUnknownLengthChunkedStyleBodySucceeds(t *testing.T) {
	fp := &fakeProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := produceRequestWithBody(&chunkedReader{
		data:  []byte(`{"records":[{"value":{"ok":true}}]}`),
		chunk: 5,
	})
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(fp.records) != 1 {
		t.Fatalf("producer records = %d, want 1", len(fp.records))
	}
	if string(fp.records[0].Value) != `{"ok":true}` {
		t.Fatalf("record value = %q", string(fp.records[0].Value))
	}
}

func TestBodyOverMaxBytesReturnsPayloadTooLargeAndSkipsProducer(t *testing.T) {
	fp := &fakeProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: int64(len(`{"records":[{"value":{"ok":true}}]}`) - 1),
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := produceRequestWithBody(strings.NewReader(`{"records":[{"value":{"ok":true}}]}`))
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(fp.records) != 0 {
		t.Fatalf("producer was called with %d records", len(fp.records))
	}
}

func TestSlowReaderEventuallyCompletesAndProduces(t *testing.T) {
	fp := &fakeProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := produceRequestWithBody(&slowReader{
		reader: &chunkedReader{
			data:  []byte(`{"records":[{"value":{"slow":true}}]}`),
			chunk: 3,
		},
		delay: 100 * time.Microsecond,
	})
	req.ContentLength = -1
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(fp.records) != 1 {
		t.Fatalf("producer records = %d, want 1", len(fp.records))
	}
	if string(fp.records[0].Value) != `{"slow":true}` {
		t.Fatalf("record value = %q", string(fp.records[0].Value))
	}
}

func TestBodyReadErrorReturnsControlledBadRequestAndSkipsProducer(t *testing.T) {
	fp := &fakeProducer{}
	h := NewHandler(fp, nil, Config{
		MaxRequestBytes: 1024,
		MaxRecords:      10,
		ProduceTimeout:  time.Second,
	}, nil)

	req := produceRequestWithBody(&errorAfterReader{
		first: []byte(`{"records":[`),
		err:   errClientDisconnected,
	})
	req.ContentLength = -1
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "failed to read request body") {
		t.Fatalf("body = %s, want controlled read-error message", rec.Body.String())
	}
	if len(fp.records) != 0 {
		t.Fatalf("producer was called with %d records", len(fp.records))
	}
}

func TestRequestBodyIsNotReadBeforePreBodyValidation(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		headers    map[string]string
		wantStatus int
	}{
		{
			name:       "method_not_allowed",
			method:     http.MethodGet,
			path:       "/topics/orders",
			headers:    map[string]string{"Content-Type": mediaKafkaJSONV2},
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "path_not_found",
			method:     http.MethodPost,
			path:       "/topics/orders/extra",
			headers:    map[string]string{"Content-Type": mediaKafkaJSONV2},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_accept",
			method:     http.MethodPost,
			path:       "/topics/orders",
			headers:    map[string]string{"Content-Type": mediaKafkaJSONV2, "Accept": "application/xml"},
			wantStatus: http.StatusNotAcceptable,
		},
		{
			name:       "bad_content_type",
			method:     http.MethodPost,
			path:       "/topics/orders",
			headers:    map[string]string{"Content-Type": "text/plain"},
			wantStatus: http.StatusUnsupportedMediaType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakeProducer{}
			h := NewHandler(fp, nil, Config{
				MaxRequestBytes: 1024,
				MaxRecords:      10,
				ProduceTimeout:  time.Second,
			}, nil)
			body := &countingReadCloser{reader: strings.NewReader(`{"records":[{"value":{"ok":true}}]}`)}
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Body = body
			req.ContentLength = -1
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()

			h.Routes().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d body=%s, want %d", rec.Code, rec.Body.String(), tc.wantStatus)
			}
			if body.reads != 0 {
				t.Fatalf("body was read %d times before pre-body validation rejected request", body.reads)
			}
			if len(fp.records) != 0 {
				t.Fatalf("producer was called with %d records", len(fp.records))
			}
		})
	}
}

func produceRequestWithBody(body io.Reader) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/topics/orders", body)
	req.Header.Set("Content-Type", mediaKafkaJSONV2)
	req.Header.Set("Accept", mediaKafkaV2)
	return req
}

type chunkedReader struct {
	data  []byte
	chunk int
	pos   int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	chunk := r.chunk
	if chunk <= 0 || chunk > len(p) {
		chunk = len(p)
	}
	remaining := len(r.data) - r.pos
	if chunk > remaining {
		chunk = remaining
	}
	copy(p, r.data[r.pos:r.pos+chunk])
	r.pos += chunk
	return chunk, nil
}

type slowReader struct {
	reader io.Reader
	delay  time.Duration
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	return r.reader.Read(p)
}

var errClientDisconnected = errors.New("client disconnected")

type errorAfterReader struct {
	first []byte
	err   error
	done  bool
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if !r.done {
		r.done = true
		return copy(p, r.first), nil
	}
	return 0, r.err
}

type countingReadCloser struct {
	reader io.Reader
	reads  int
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	r.reads++
	return r.reader.Read(p)
}

func (r *countingReadCloser) Close() error {
	return nil
}
