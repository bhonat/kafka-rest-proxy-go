package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Registry interface {
	Resolve(ctx context.Context, req ResolveRequest) (Resolved, error)
}

type HTTPRegistry struct {
	baseURL  *url.URL
	client   *http.Client
	username string
	password string
}

func NewHTTPRegistry(rawURL, username, password string) (*HTTPRegistry, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("schema registry URL must include scheme and host")
	}
	return &HTTPRegistry{
		baseURL:  u,
		client:   &http.Client{Timeout: 10 * time.Second},
		username: username,
		password: password,
	}, nil
}

func (r *HTTPRegistry) Resolve(ctx context.Context, req ResolveRequest) (Resolved, error) {
	if r == nil {
		return Resolved{}, fmt.Errorf("schema registry is not configured")
	}
	if strings.TrimSpace(req.Subject) == "" && req.SchemaID == nil {
		return Resolved{}, fmt.Errorf("schema subject is required")
	}

	if strings.TrimSpace(req.Schema) != "" {
		return r.register(ctx, req)
	}
	if req.SchemaVersion != nil {
		return r.getSubjectVersion(ctx, req.Subject, strconv.Itoa(*req.SchemaVersion))
	}
	if req.SchemaID != nil {
		return r.getSchemaByID(ctx, *req.SchemaID, req.Type, req.Subject)
	}
	return r.getSubjectVersion(ctx, req.Subject, "latest")
}

func (r *HTTPRegistry) register(ctx context.Context, req ResolveRequest) (Resolved, error) {
	body := map[string]string{"schema": req.Schema}
	if registryType := registrySchemaType(req.Type); registryType != "" {
		body["schemaType"] = registryType
	}
	var resp struct {
		ID int `json:"id"`
	}
	if err := r.doJSON(ctx, http.MethodPost, "/subjects/"+url.PathEscape(req.Subject)+"/versions", body, &resp); err != nil {
		return Resolved{}, err
	}
	return Resolved{
		Subject:  req.Subject,
		Type:     normalizeSchemaType(req.Type),
		SchemaID: resp.ID,
		Schema:   req.Schema,
	}, nil
}

func (r *HTTPRegistry) getSubjectVersion(ctx context.Context, subject, version string) (Resolved, error) {
	var resp struct {
		Subject    string `json:"subject"`
		ID         int    `json:"id"`
		Version    int    `json:"version"`
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := r.doJSON(ctx, http.MethodGet, "/subjects/"+url.PathEscape(subject)+"/versions/"+url.PathEscape(version), nil, &resp); err != nil {
		return Resolved{}, err
	}
	return Resolved{
		Subject:       firstNonEmpty(resp.Subject, subject),
		Type:          normalizeRegistrySchemaType(resp.SchemaType),
		SchemaID:      resp.ID,
		SchemaVersion: &resp.Version,
		Schema:        resp.Schema,
	}, nil
}

func (r *HTTPRegistry) getSchemaByID(ctx context.Context, id int, typ, subject string) (Resolved, error) {
	var resp struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := r.doJSON(ctx, http.MethodGet, "/schemas/ids/"+strconv.Itoa(id), nil, &resp); err != nil {
		return Resolved{}, err
	}
	return Resolved{
		Subject:  subject,
		Type:     firstNonEmpty(normalizeRegistrySchemaType(resp.SchemaType), normalizeSchemaType(typ)),
		SchemaID: id,
		Schema:   resp.Schema,
	}, nil
}

func (r *HTTPRegistry) doJSON(ctx context.Context, method, suffix string, in any, out any) error {
	u := *r.baseURL
	u.Path = path.Join(r.baseURL.Path, suffix)
	var body *bytes.Reader
	if in == nil {
		body = bytes.NewReader(nil)
	} else {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	if r.username != "" || r.password != "" {
		req.SetBasicAuth(r.username, r.password)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var er struct {
			ErrorCode int    `json:"error_code"`
			Message   string `json:"message"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&er)
		if er.Message == "" {
			er.Message = resp.Status
		}
		return fmt.Errorf("schema registry %s %s: %s", method, suffix, er.Message)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type MemoryRegistry struct {
	mu       sync.Mutex
	nextID   int
	byID     map[int]Resolved
	versions map[string][]Resolved
}

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		nextID:   1,
		byID:     map[int]Resolved{},
		versions: map[string][]Resolved{},
	}
}

func (r *MemoryRegistry) Resolve(_ context.Context, req ResolveRequest) (Resolved, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextID == 0 {
		r.nextID = 1
	}
	if req.SchemaID != nil {
		res, ok := r.byID[*req.SchemaID]
		if !ok {
			return Resolved{}, fmt.Errorf("schema id %d not found", *req.SchemaID)
		}
		if req.Subject != "" {
			res.Subject = req.Subject
		}
		return res, nil
	}
	if req.Subject == "" {
		return Resolved{}, fmt.Errorf("schema subject is required")
	}
	if req.SchemaVersion != nil {
		versions := r.versions[req.Subject]
		idx := *req.SchemaVersion - 1
		if idx < 0 || idx >= len(versions) {
			return Resolved{}, fmt.Errorf("schema subject %q version %d not found", req.Subject, *req.SchemaVersion)
		}
		return versions[idx], nil
	}
	if req.Schema != "" {
		version := len(r.versions[req.Subject]) + 1
		id := r.nextID
		r.nextID++
		res := Resolved{
			Subject:       req.Subject,
			Type:          normalizeSchemaType(req.Type),
			SchemaID:      id,
			SchemaVersion: &version,
			Schema:        req.Schema,
		}
		r.byID[id] = res
		r.versions[req.Subject] = append(r.versions[req.Subject], res)
		return res, nil
	}
	versions := r.versions[req.Subject]
	if len(versions) == 0 {
		return Resolved{}, fmt.Errorf("schema subject %q not found", req.Subject)
	}
	return versions[len(versions)-1], nil
}

func registrySchemaType(typ string) string {
	switch normalizeSchemaType(typ) {
	case TypeJSONSchema:
		return "JSON"
	case TypeProtobuf:
		return "PROTOBUF"
	case TypeAvro:
		return "AVRO"
	default:
		return ""
	}
}

func normalizeRegistrySchemaType(typ string) string {
	switch strings.ToUpper(strings.TrimSpace(typ)) {
	case "JSON":
		return TypeJSONSchema
	case "PROTOBUF":
		return TypeProtobuf
	case "AVRO", "":
		return TypeAvro
	default:
		return strings.ToUpper(strings.TrimSpace(typ))
	}
}

func normalizeSchemaType(typ string) string {
	switch strings.ToUpper(strings.TrimSpace(typ)) {
	case TypeJSONSchema, "JSON_SCHEMA", "JSON":
		return TypeJSONSchema
	case TypeProtobuf, "PROTO":
		return TypeProtobuf
	case TypeAvro, "":
		return TypeAvro
	default:
		return strings.ToUpper(strings.TrimSpace(typ))
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
