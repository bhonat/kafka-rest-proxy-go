package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type captureRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type captureResponse struct {
	Status     int               `json:"status"`
	Headers    map[string]string `json:"headers"`
	Body       json.RawMessage   `json:"body,omitempty"`
	BodyText   string            `json:"body_text,omitempty"`
	BodyIsJSON bool              `json:"body_is_json"`
	ElapsedMS  int64             `json:"elapsed_ms"`
	RequestErr string            `json:"request_error,omitempty"`
}

type captureCase struct {
	Name     string          `json:"name"`
	Request  captureRequest  `json:"request"`
	Response captureResponse `json:"response"`
}

type captureReport struct {
	GeneratedAt string        `json:"generated_at"`
	TargetURL   string        `json:"target_url"`
	Cases       []captureCase `json:"cases"`
}

func main() {
	var (
		baseURL = flag.String("url", "http://localhost:8082", "Confluent REST Proxy base URL")
		topic   = flag.String("topic", "orders", "existing Kafka topic for edge-case captures")
		outPath = flag.String("out", "compatibility/captured/confluent-producer-edge-cases.json", "output JSON path")
		timeout = flag.Duration("timeout", 10*time.Second, "per-request timeout")
	)
	flag.Parse()

	report := captureReport{
		GeneratedAt: time.Now().Format(time.RFC3339),
		TargetURL:   strings.TrimRight(*baseURL, "/"),
	}
	client := &http.Client{Timeout: *timeout}
	for _, tc := range captureCases(*topic) {
		report.Cases = append(report.Cases, runCase(client, report.TargetURL, tc, *timeout))
	}

	if err := writeReport(*outPath, report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("captured_cases=%d out=%s\n", len(report.Cases), *outPath)
}

func captureCases(topic string) []captureRequest {
	goodBody := `{"records":[{"value":{"ok":true}}]}`
	bigPayload := strings.Repeat("x", 2*1024*1024)
	return []captureRequest{
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "text/plain",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: goodBody,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/xml",
			},
			Body: goodBody,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape("bad topic"),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: goodBody,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: `{"records":[{"partition":999999,"value":{"ok":true}}]}`,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: `{"records":[{"value":{"payload":"` + bigPayload + `"}}]}`,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: `{"records":[`,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.binary.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: `{"records":[{"value":"not-base64!"}]}`,
		},
		{
			Method: "POST",
			Path:   "/topics/" + url.PathEscape(topic),
			Headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			Body: `{"records":[{"partition":0,"value":{"ok":true}},{"partition":999999,"value":{"ok":false}}]}`,
		},
	}
}

func runCase(client *http.Client, baseURL string, tc captureRequest, timeout time.Duration) captureCase {
	name := caseName(tc)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, tc.Method, baseURL+tc.Path, bytes.NewReader([]byte(tc.Body)))
	if err != nil {
		return captureCase{Name: name, Request: previewRequest(tc), Response: captureResponse{RequestErr: err.Error()}}
	}
	for k, v := range tc.Headers {
		req.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return captureCase{Name: name, Request: previewRequest(tc), Response: captureResponse{RequestErr: err.Error(), ElapsedMS: time.Since(start).Milliseconds()}}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return captureCase{
		Name:    name,
		Request: previewRequest(tc),
		Response: captureResponse{
			Status:     resp.StatusCode,
			Headers:    selectedHeaders(resp.Header),
			ElapsedMS:  time.Since(start).Milliseconds(),
			Body:       jsonBody(body),
			BodyText:   textBody(body),
			BodyIsJSON: json.Valid(body),
		},
	}
}

func caseName(tc captureRequest) string {
	switch {
	case tc.Headers["Content-Type"] == "text/plain":
		return "bad-content-type"
	case tc.Headers["Accept"] == "application/xml":
		return "bad-accept"
	case strings.Contains(tc.Path, "bad%20topic"):
		return "invalid-topic"
	case strings.Contains(tc.Body, "not-base64"):
		return "binary-decode-failure"
	case tc.Body == `{"records":[`:
		return "malformed-json"
	case strings.Contains(tc.Body, strings.Repeat("x", 1024)):
		return "oversized-body"
	case strings.Count(tc.Body, "partition") > 1:
		return "partial-record-errors"
	case strings.Contains(tc.Body, "999999"):
		return "invalid-partition"
	default:
		return "unknown"
	}
}

func previewRequest(tc captureRequest) captureRequest {
	tc.Body = preview(tc.Body, 2048)
	return tc
}

func preview(v string, limit int) string {
	if len(v) <= limit {
		return v
	}
	return v[:limit] + fmt.Sprintf("\n... truncated %d bytes ...", len(v)-limit)
}

func selectedHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for _, name := range []string{"Content-Type", "X-Request-Id"} {
		if v := h.Get(name); v != "" {
			out[name] = v
		}
	}
	return out
}

func jsonBody(body []byte) json.RawMessage {
	if !json.Valid(body) {
		return nil
	}
	return append(json.RawMessage(nil), body...)
}

func textBody(body []byte) string {
	if json.Valid(body) {
		return ""
	}
	return string(body)
}

func writeReport(path string, report captureReport) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
