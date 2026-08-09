package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bhonat/kafka-rest-proxy-go/internal/metrics"
	"github.com/bhonat/kafka-rest-proxy-go/internal/producer"
	"github.com/bhonat/kafka-rest-proxy-go/internal/ratelimit"
	schemaproducer "github.com/bhonat/kafka-rest-proxy-go/internal/schema"
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
	ClusterID       string

	RateLimitRequestsPerSecond float64
	RateLimitRequestsBurst     int64
	RateLimitBytesPerSecond    float64
	RateLimitBytesBurst        int64
}

type Handler struct {
	producer             Producer
	metrics              *metrics.Metrics
	cfg                  Config
	log                  *slog.Logger
	tokens               map[string]struct{}
	allowedTopics        map[string]struct{}
	allowedTopicPrefixes []string
	requestRateLimiter   *ratelimit.Limiter
	byteRateLimiter      *ratelimit.Limiter
	schemaEncoder        *schemaproducer.Encoder
}

type Producer interface {
	Produce(ctx context.Context, records []producer.Record) ([]producer.Result, error)
	Ping(ctx context.Context) error
}

func NewHandler(p Producer, m *metrics.Metrics, cfg Config, log *slog.Logger) *Handler {
	return NewHandlerWithSchema(p, m, cfg, log, nil)
}

func NewHandlerWithSchema(p Producer, m *metrics.Metrics, cfg Config, log *slog.Logger, enc *schemaproducer.Encoder) *Handler {
	if log == nil {
		log = slog.Default()
	}
	cfg = cfg.withDefaults()
	h := &Handler{
		producer:      p,
		metrics:       m,
		cfg:           cfg,
		log:           log,
		schemaEncoder: enc,
	}
	if cfg.RateLimitRequestsPerSecond > 0 {
		burst := cfg.RateLimitRequestsBurst
		if burst <= 0 {
			burst = int64(math.Ceil(cfg.RateLimitRequestsPerSecond))
		}
		h.requestRateLimiter = ratelimit.New(cfg.RateLimitRequestsPerSecond, burst)
	}
	if cfg.RateLimitBytesPerSecond > 0 {
		burst := cfg.RateLimitBytesBurst
		if burst <= 0 {
			burst = maxInt64(int64(math.Ceil(cfg.RateLimitBytesPerSecond)), cfg.MaxRequestBytes)
		}
		h.byteRateLimiter = ratelimit.New(cfg.RateLimitBytesPerSecond, burst)
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
	mux.HandleFunc("/v3/clusters/", h.handleV3Produce)
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

	target, ok := produceTargetFromPath(r.URL)
	if !ok {
		status = http.StatusNotFound
		writeAPIError(w, status, errorCodeBadRequest, "not found")
		return
	}
	topic := target.topic
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

	if !h.allowRequestRate(1) {
		status = http.StatusTooManyRequests
		if h.metrics != nil {
			h.metrics.ObserveRateLimitRejected("requests")
		}
		writeRateLimitExceeded(w)
		return
	}

	body, err := readRequestBody(w, r, h.cfg.MaxRequestBytes)
	bodyLen = int64(len(body))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			status = http.StatusRequestEntityTooLarge
			writeAPIError(w, status, http.StatusRequestEntityTooLarge, "HTTP 413 Payload Too Large")
			return
		}
		status = http.StatusBadRequest
		writeAPIError(w, status, errorCodeBadRequest, "failed to read request body")
		return
	}

	if !h.allowByteRate(bodyLen) {
		status = http.StatusTooManyRequests
		if h.metrics != nil {
			h.metrics.ObserveRateLimitRejected("bytes")
		}
		writeRateLimitExceeded(w)
		return
	}

	decodeStart := time.Now()
	records, responseMeta, err := decodeProduceRequestWithSchema(r.Context(), topic, body, format, h.cfg.decodeLimits(), h.schemaEncoder, target.partition)
	if h.metrics != nil {
		h.metrics.ObserveDecode(format.String(), err == nil, time.Since(decodeStart))
	}
	if err != nil {
		status = http.StatusBadRequest
		var ue unprocessableError
		if errors.As(err, &ue) {
			status = http.StatusUnprocessableEntity
			writeAPIError(w, status, errorCodeUnprocessable, ue.Error())
			return
		}
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
			err = errContextCanceled
		}
		var code int
		var msg string
		status, code, msg = classifyProduceError(err)
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
	_, _ = w.Write(appendProduceResponse(nil, results, responseMeta))
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

type produceTarget struct {
	topic     string
	partition *int32
}

func produceTargetFromPath(u *url.URL) (produceTarget, bool) {
	const prefix = "/topics/"
	path := u.EscapedPath()
	if !strings.HasPrefix(path, prefix) || len(path) == len(prefix) {
		return produceTarget{}, false
	}
	rest := strings.TrimPrefix(path, prefix)
	segments := strings.Split(rest, "/")
	if len(segments) != 1 && len(segments) != 3 {
		return produceTarget{}, false
	}

	topic, err := url.PathUnescape(segments[0])
	if err != nil || topic == "" {
		return produceTarget{}, false
	}
	if len(segments) == 1 {
		return produceTarget{topic: topic}, true
	}
	if segments[1] != "partitions" || segments[2] == "" {
		return produceTarget{}, false
	}
	partition, err := strconv.ParseInt(segments[2], 10, 32)
	if err != nil || partition < 0 {
		return produceTarget{}, false
	}
	partition32 := int32(partition)
	return produceTarget{topic: topic, partition: &partition32}, true
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

func (h *Handler) allowRequestRate(cost int64) bool {
	if h.requestRateLimiter == nil {
		return true
	}
	return h.requestRateLimiter.Allow(cost)
}

func (h *Handler) allowByteRate(cost int64) bool {
	if h.byteRateLimiter == nil {
		return true
	}
	return h.byteRateLimiter.Allow(cost)
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
	if strings.TrimSpace(cfg.ClusterID) == "" {
		cfg.ClusterID = "local"
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
