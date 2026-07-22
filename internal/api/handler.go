package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"
	"time"

	"github.com/example/kafka-rest-proxy-go/internal/metrics"
	"github.com/example/kafka-rest-proxy-go/internal/producer"
)

type Config struct {
	MaxRequestBytes int64
	MaxRecords      int
	MaxRecordBytes  int64
	MaxKeyBytes     int64
	MaxHeaders      int
	MaxHeaderBytes  int64
	AllowedTopics   []string
	ProduceTimeout  time.Duration
	BearerTokens    []string
	PprofEnable     bool
}

type Handler struct {
	producer             Producer
	metrics              *metrics.Metrics
	cfg                  Config
	log                  *slog.Logger
	tokens               map[string]struct{}
	allowedTopics        map[string]struct{}
	allowedTopicPrefixes []string
}

type Producer interface {
	Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error)
	Ping(ctx context.Context) error
}

func NewHandler(p Producer, m *metrics.Metrics, cfg Config, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	cfg = cfg.withDefaults()
	h := &Handler{
		producer: p,
		metrics:  m,
		cfg:      cfg,
		log:      log,
	}
	if len(cfg.BearerTokens) > 0 {
		h.tokens = make(map[string]struct{}, len(cfg.BearerTokens))
		for _, token := range cfg.BearerTokens {
			token = strings.TrimSpace(token)
			if token != "" {
				h.tokens[token] = struct{}{}
			}
		}
	}
	if len(cfg.AllowedTopics) > 0 {
		h.allowedTopics = make(map[string]struct{}, len(cfg.AllowedTopics))
		for _, topic := range cfg.AllowedTopics {
			topic = strings.TrimSpace(topic)
			if topic != "" {
				if strings.HasSuffix(topic, "*") {
					prefix := strings.TrimSuffix(topic, "*")
					if prefix != "" {
						h.allowedTopicPrefixes = append(h.allowedTopicPrefixes, prefix)
					}
				} else {
					h.allowedTopics[topic] = struct{}{}
				}
			}
		}
	}
	return h
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/topics/", h.handleTopicProduce)
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/readyz", h.handleReady)
	if h.metrics != nil {
		mux.Handle("/metrics", h.metrics.Handler())
	}
	if h.cfg.PprofEnable {
		mux.HandleFunc("/debug/pprof", pprof.Index)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return h.authMiddleware(mux)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeAPIError(w, http.StatusMethodNotAllowed, errorCodeBadRequest, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeAPIError(w, http.StatusMethodNotAllowed, errorCodeBadRequest, "method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.producer.Ping(ctx); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, errorCodeProduceUnavailable, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (h *Handler) handleTopicProduce(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := http.StatusOK
	var bodyLen int64
	var recordCount int

	if h.metrics != nil {
		h.metrics.IncOutstanding()
		defer h.metrics.DecOutstanding()
		defer func() {
			h.metrics.ObserveRequest(status, bodyLen, recordCount, time.Since(start))
		}()
	}

	if r.Method != http.MethodPost {
		status = http.StatusMethodNotAllowed
		w.Header().Set("Allow", http.MethodPost)
		writeAPIError(w, status, errorCodeBadRequest, "method not allowed")
		return
	}

	topic, ok := topicFromPath(r.URL)
	if !ok {
		status = http.StatusNotFound
		writeAPIError(w, status, errorCodeBadRequest, "not found")
		return
	}
	if !h.topicAllowed(topic) {
		status = http.StatusForbidden
		writeAPIError(w, status, errorCodeForbidden, "topic is not allowed")
		return
	}

	if !acceptsResponse(r) {
		status = http.StatusNotAcceptable
		writeAPIError(w, status, errorCodeNotAcceptable, "HTTP 406 Not Acceptable")
		return
	}

	format, ok := parseContentType(r.Header.Get("Content-Type"))
	if !ok {
		status = http.StatusUnsupportedMediaType
		writeAPIError(w, status, errorCodeUnsupportedMedia, "HTTP 415 Unsupported Media Type")
		return
	}

	body, err := readRequestBody(w, r, h.cfg.MaxRequestBytes)
	bodyLen = int64(len(body))
	if err != nil {
		status = http.StatusRequestEntityTooLarge
		writeAPIError(w, status, http.StatusRequestEntityTooLarge, "HTTP 413 Payload Too Large")
		return
	}

	decodeStart := time.Now()
	records, err := decodeProduceRequest(topic, body, format, h.cfg.decodeLimits())
	if h.metrics != nil {
		h.metrics.ObserveDecode(format.String(), err == nil, time.Since(decodeStart))
	}
	if err != nil {
		status = http.StatusBadRequest
		var ve validationError
		if errors.As(err, &ve) {
			writeAPIError(w, status, errorCodeBadRequest, ve.Error())
			return
		}
		writeAPIError(w, status, errorCodeBadRequest, err.Error())
		return
	}
	recordCount = len(records)

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ProduceTimeout)
	defer cancel()

	produceStart := time.Now()
	results, err := h.producer.Produce(ctx, records)
	produceWait := time.Since(produceStart)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			err = contextCanceled
		}
		status, code, msg := classifyProduceError(err)
		if h.metrics != nil {
			h.metrics.ObserveKafkaCallbackWait(status, produceWait)
			if errors.Is(err, producer.ErrOverloaded) {
				h.metrics.ObserveAdmissionRejected()
			}
		}
		writeAPIError(w, status, code, msg)
		return
	}

	if h.metrics != nil {
		h.metrics.ObserveKafkaCallbackWait(status, produceWait)
		successes, failures := countResultStatus(results)
		h.metrics.ObserveProduceResult(successes, failures)
	}

	w.Header().Set("Content-Type", mediaKafkaV2)
	w.WriteHeader(status)
	_, _ = w.Write(appendProduceResponse(nil, results))
}

func readRequestBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	bodyReader := http.MaxBytesReader(w, r.Body, maxBytes)
	if r.ContentLength > 0 && r.ContentLength <= maxBytes {
		var buf bytes.Buffer
		buf.Grow(int(r.ContentLength))
		if _, err := buf.ReadFrom(bodyReader); err != nil {
			return buf.Bytes(), err
		}
		return buf.Bytes(), nil
	}
	return io.ReadAll(bodyReader)
}

func topicFromPath(u *url.URL) (string, bool) {
	const prefix = "/topics/"
	path := u.EscapedPath()
	if !strings.HasPrefix(path, prefix) || len(path) == len(prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	if strings.Contains(rest, "/") {
		return "", false
	}
	topic, err := url.PathUnescape(rest)
	if err != nil || topic == "" {
		return "", false
	}
	return topic, true
}

func (h *Handler) authMiddleware(next http.Handler) http.Handler {
	if len(h.tokens) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			writeAPIError(w, http.StatusUnauthorized, errorCodeUnauthorized, "missing bearer token")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
		if _, ok := h.tokens[token]; !ok {
			writeAPIError(w, http.StatusUnauthorized, errorCodeUnauthorized, "invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) topicAllowed(topic string) bool {
	if len(h.allowedTopics) == 0 && len(h.allowedTopicPrefixes) == 0 {
		return true
	}
	if _, ok := h.allowedTopics[topic]; ok {
		return true
	}
	for _, prefix := range h.allowedTopicPrefixes {
		if strings.HasPrefix(topic, prefix) {
			return true
		}
	}
	return false
}

func (cfg Config) withDefaults() Config {
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = 8 * 1024 * 1024
	}
	if cfg.MaxRecords <= 0 {
		cfg.MaxRecords = 1000
	}
	if cfg.MaxRecordBytes <= 0 {
		cfg.MaxRecordBytes = 1024 * 1024
	}
	if cfg.MaxKeyBytes <= 0 {
		cfg.MaxKeyBytes = 1024 * 1024
	}
	if cfg.MaxHeaders <= 0 {
		cfg.MaxHeaders = 64
	}
	if cfg.MaxHeaderBytes <= 0 {
		cfg.MaxHeaderBytes = 64 * 1024
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = 30 * time.Second
	}
	return cfg
}

func (cfg Config) decodeLimits() decodeLimits {
	return decodeLimits{
		MaxRecords:     cfg.MaxRecords,
		MaxRecordBytes: cfg.MaxRecordBytes,
		MaxKeyBytes:    cfg.MaxKeyBytes,
		MaxHeaders:     cfg.MaxHeaders,
		MaxHeaderBytes: cfg.MaxHeaderBytes,
	}
}
