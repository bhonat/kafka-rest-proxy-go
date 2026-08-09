package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHTTPRegistryRegistersSchema(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	reg, err := NewHTTPRegistry("http://schema-registry.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	reg.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		return jsonResponse(map[string]int{"id": 7}), nil
	})}

	resolved, err := reg.Resolve(context.Background(), ResolveRequest{
		Subject: "orders-value",
		Type:    TypeAvro,
		Schema:  `{"type":"string"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/subjects/orders-value/versions" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotBody["schemaType"] != "AVRO" || gotBody["schema"] != `{"type":"string"}` {
		t.Fatalf("body = %+v", gotBody)
	}
	if resolved.SchemaID != 7 || resolved.Subject != "orders-value" || resolved.Type != TypeAvro {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestHTTPRegistryFetchesSchemaByID(t *testing.T) {
	reg, err := NewHTTPRegistry("http://schema-registry.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	reg.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/schemas/ids/7" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		return jsonResponse(map[string]string{
			"schemaType": "JSON",
			"schema":     `{"type":"object"}`,
		}), nil
	})}

	id := 7
	resolved, err := reg.Resolve(context.Background(), ResolveRequest{
		Subject:  "orders-value",
		SchemaID: &id,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.SchemaID != 7 || resolved.Type != TypeJSONSchema || !strings.Contains(resolved.Schema, "object") {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func jsonResponse(v any) *http.Response {
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(buf.Bytes())),
	}
}
