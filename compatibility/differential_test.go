package compatibility_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	differentialEnabledEnv     = "KAFKA_REST_DIFFERENTIAL"
	differentialGoURLEnv       = "KAFKA_REST_GO_URL"
	differentialConfluentEnv   = "KAFKA_REST_CONFLUENT_URL"
	differentialTopicEnv       = "KAFKA_REST_DIFFERENTIAL_TOPIC"
	differentialBinaryTopicEnv = "KAFKA_REST_DIFFERENTIAL_BINARY_TOPIC"
	differentialEdgeEnv        = "KAFKA_REST_DIFFERENTIAL_EDGE"
	differentialTimeoutEnv     = "KAFKA_REST_DIFFERENTIAL_TIMEOUT"
)

type differentialCase struct {
	name               string
	method             string
	path               string
	headers            map[string]string
	body               string
	compareContentType bool
	strict             bool
}

type differentialResponse struct {
	status      int
	contentType string
	body        []byte
	requestErr  string
}

type normalizedDifferentialResponse struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type,omitempty"`
	RequestErr  bool   `json:"request_error,omitempty"`
	Body        any    `json:"body,omitempty"`
	BodyText    string `json:"body_text,omitempty"`
}

func TestDifferentialProducerCompatibility(t *testing.T) {
	if os.Getenv(differentialEnabledEnv) != "1" {
		t.Skipf("set %s=1 to run live differential tests against Go and Confluent REST proxies", differentialEnabledEnv)
	}

	goURL := envOrDefault(differentialGoURLEnv, "http://localhost:8080")
	confluentURL := envOrDefault(differentialConfluentEnv, "http://localhost:8082")
	topic := envOrDefault(differentialTopicEnv, "orders")
	binaryTopic := envOrDefault(differentialBinaryTopicEnv, "binary-events")
	timeout := envDurationOrDefault(differentialTimeoutEnv, 10*time.Second)

	client := &http.Client{Timeout: timeout}
	cases := differentialCases(topic, binaryTopic)
	if os.Getenv(differentialEdgeEnv) == "1" {
		cases = append(cases, differentialEdgeCases(topic, binaryTopic)...)
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			goResp := runDifferentialRequest(ctx, client, goURL, tc)
			confluentResp := runDifferentialRequest(ctx, client, confluentURL, tc)

			normalizedGo := normalizeDifferentialResponse(goResp, tc.compareContentType)
			normalizedConfluent := normalizeDifferentialResponse(confluentResp, tc.compareContentType)
			if !reflect.DeepEqual(normalizedGo, normalizedConfluent) {
				goPretty := mustMarshalJSON(normalizedGo)
				confluentPretty := mustMarshalJSON(normalizedConfluent)
				msg := fmt.Sprintf("normalized differential mismatch\nGo proxy:\n%s\nConfluent REST Proxy:\n%s", goPretty, confluentPretty)
				if tc.strict {
					t.Fatal(msg)
				}
				t.Log(msg)
			}
		})
	}
}

func differentialCases(topic, binaryTopic string) []differentialCase {
	jsonHeaders := map[string]string{
		"Content-Type": "application/vnd.kafka.json.v2+json",
		"Accept":       "application/vnd.kafka.v2+json",
	}
	binaryHeaders := map[string]string{
		"Content-Type": "application/vnd.kafka.binary.v2+json",
		"Accept":       "application/vnd.kafka.v2+json",
	}

	return []differentialCase{
		{
			name:               "json-success",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[{"partition":0,"key":"customer-123","value":{"order_id":"order-456","amount":42}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "json-empty-records",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[]}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:               "json-missing-records",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:               "json-null-records",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":null}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:               "json-missing-value",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[{"partition":0,"key":"customer-123"}]}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:               "json-key-missing",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[{"partition":0,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "json-key-null",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[{"partition":0,"key":null,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "json-value-null",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[{"partition":0,"key":"tombstone-key","value":null}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:   "json-content-type-with-charset",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json; charset=utf-8",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"partition":0,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:   "json-accept-wildcard",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "*/*",
			},
			body:               `{"records":[{"partition":0,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:   "json-default-accept",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
			},
			body:               `{"records":[{"partition":0,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "json-larger-batch-10-records",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               jsonProduceBatch(10),
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "binary-success",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(binaryTopic),
			headers:            binaryHeaders,
			body:               `{"records":[{"partition":0,"key":"Y3VzdG9tZXItMTIz","value":"aGVsbG8td29ybGQ="}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "binary-key-value-null",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(binaryTopic),
			headers:            binaryHeaders,
			body:               `{"records":[{"partition":0,"key":null,"value":null}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "binary-bad-base64",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(binaryTopic),
			headers:            binaryHeaders,
			body:               `{"records":[{"partition":0,"value":"not-base64!"}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:               "malformed-json",
			method:             http.MethodPost,
			path:               "/topics/" + url.PathEscape(topic),
			headers:            jsonHeaders,
			body:               `{"records":[`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:   "unsupported-media-type",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "text/plain",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             true,
		},
		{
			name:   "bad-accept",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/xml",
			},
			body: `{"records":[{"value":{"ok":true}}]}`,
			// Confluent may omit Content-Type on 406 while the Go proxy returns the
			// Kafka v2 media type. Compare the API error contract, not that header.
			compareContentType: false,
			strict:             true,
		},
	}
}

func differentialEdgeCases(topic, binaryTopic string) []differentialCase {
	return []differentialCase{
		{
			name:   "invalid-topic-diagnostic",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape("bad topic"),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:   "negative-partition-diagnostic",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"partition":-1,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:   "invalid-partition-diagnostic",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"partition":999999,"value":{"ok":true}}]}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:   "json-headers-diagnostic",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(topic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.json.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"partition":0,"value":{"ok":true},"headers":[{"key":"trace-id","value":"trace-123"}]}]}`,
			compareContentType: true,
			strict:             false,
		},
		{
			name:   "binary-headers-diagnostic",
			method: http.MethodPost,
			path:   "/topics/" + url.PathEscape(binaryTopic),
			headers: map[string]string{
				"Content-Type": "application/vnd.kafka.binary.v2+json",
				"Accept":       "application/vnd.kafka.v2+json",
			},
			body:               `{"records":[{"partition":0,"value":"aGVsbG8=","headers":[{"key":"trace-id","value":"dHJhY2UtMTIz"}]}]}`,
			compareContentType: true,
			strict:             false,
		},
	}
}

func jsonProduceBatch(records int) string {
	var b strings.Builder
	b.WriteString(`{"records":[`)
	for i := 0; i < records; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&b, `{"partition":0,"key":"batch-key-%d","value":{"i":%d,"ok":true}}`, i, i)
	}
	b.WriteString(`]}`)
	return b.String()
}

func runDifferentialRequest(ctx context.Context, client *http.Client, baseURL string, tc differentialCase) differentialResponse {
	req, err := http.NewRequestWithContext(ctx, tc.method, strings.TrimRight(baseURL, "/")+tc.path, bytes.NewReader([]byte(tc.body)))
	if err != nil {
		return differentialResponse{requestErr: err.Error()}
	}
	for k, v := range tc.headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return differentialResponse{requestErr: err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return differentialResponse{status: resp.StatusCode, contentType: responseMediaType(resp.Header.Get("Content-Type")), requestErr: err.Error()}
	}
	return differentialResponse{
		status:      resp.StatusCode,
		contentType: responseMediaType(resp.Header.Get("Content-Type")),
		body:        body,
	}
}

func normalizeDifferentialResponse(resp differentialResponse, includeContentType bool) normalizedDifferentialResponse {
	out := normalizedDifferentialResponse{
		Status:     resp.status,
		RequestErr: resp.requestErr != "",
	}
	if includeContentType {
		out.ContentType = resp.contentType
	}
	if resp.requestErr != "" {
		return out
	}
	if len(bytes.TrimSpace(resp.body)) == 0 {
		return out
	}

	var body any
	if err := json.Unmarshal(resp.body, &body); err != nil {
		out.BodyText = "<non-json>"
		return out
	}
	out.Body = normalizeJSONValue(body)
	return out
}

func normalizeJSONValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			switch k {
			case "offset":
				if v != nil {
					out[k] = "<offset>"
				} else {
					out[k] = nil
				}
			case "message":
				if s, ok := v.(string); ok && s != "" {
					out[k] = "<message>"
				} else {
					out[k] = v
				}
			case "error":
				if s, ok := v.(string); ok && s != "" {
					out[k] = "<error>"
				} else {
					out[k] = v
				}
			default:
				out[k] = normalizeJSONValue(v)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = normalizeJSONValue(v)
		}
		return out
	default:
		return x
	}
}

func responseMediaType(contentType string) string {
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = contentType[:i]
	}
	return strings.TrimSpace(strings.ToLower(contentType))
}

func envOrDefault(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func envDurationOrDefault(name string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func mustMarshalJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("<json marshal failed: %v>", err)
	}
	return string(b)
}
