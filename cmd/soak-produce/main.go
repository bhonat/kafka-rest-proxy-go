package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type options struct {
	URL               string
	Topic             string
	Duration          time.Duration
	Warmup            time.Duration
	Clients           int
	RecordsPerRequest int
	PayloadBytes      int
	Format            string
	Timeout           time.Duration
	MaxLatencySamples int
	Thresholds        thresholds
}

type thresholds struct {
	MaxFailureRate      float64
	MinRecordsPerSecond float64
	MaxP99              time.Duration
}

type produceRequest struct {
	Records []produceRecord `json:"records"`
}

type produceRecord struct {
	Value any `json:"value"`
}

type produceResponse struct {
	Offsets []produceOffset `json:"offsets"`
}

type produceOffset struct {
	ErrorCode *int    `json:"error_code"`
	Error     *string `json:"error"`
}

type requestResult struct {
	Status         int
	Records        int64
	SuccessRecords int64
	FailedRecords  int64
	Bytes          int64
	Latency        time.Duration
	Err            error
	FailureReason  string
}

type failureCount struct {
	Reason string
	Count  int64
}

type summary struct {
	URL                string
	Topic              string
	Duration           time.Duration
	Elapsed            time.Duration
	Clients            int
	RecordsPerRequest  int
	PayloadBytes       int
	Format             string
	TotalRequests      int64
	SuccessRequests    int64
	FailedRequests     int64
	AttemptedRecords   int64
	SuccessRecords     int64
	FailedRecords      int64
	RequestBytes       int64
	RequestsPerSecond  float64
	RecordsPerSecond   float64
	RequestFailureRate float64
	RecordFailureRate  float64
	P50                time.Duration
	P95                time.Duration
	P99                time.Duration
	LatencySamples     int
	FailureBreakdown   []failureCount
}

type violation struct {
	Name      string
	Actual    string
	Threshold string
}

func main() {
	opts, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	body, err := buildBody(opts.RecordsPerRequest, opts.PayloadBytes, opts.Format)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if opts.Warmup > 0 {
		warmupOpts := opts
		warmupOpts.Duration = opts.Warmup
		warmupSummary := runSoak(warmupOpts, body)
		fmt.Printf("warmup_complete elapsed=%s requests=%d records=%d records_per_sec=%.2f failure_rate=%.4f%% p99=%s\n",
			warmupSummary.Elapsed.Round(time.Millisecond),
			warmupSummary.TotalRequests,
			warmupSummary.SuccessRecords,
			warmupSummary.RecordsPerSecond,
			warmupSummary.RecordFailureRate*100,
			warmupSummary.P99.Round(time.Millisecond),
		)
	}

	result := runSoak(opts, body)
	violations := evaluateThresholds(result, opts.Thresholds)
	printSummary(result, opts.Thresholds, violations)
	if len(violations) > 0 {
		os.Exit(1)
	}
}

func parseOptions(args []string) (options, error) {
	fs := flag.NewFlagSet("soak-produce", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		rawURL            = fs.String("url", "http://localhost:8080", "REST proxy base URL")
		topic             = fs.String("topic", "orders", "Kafka topic")
		duration          = fs.Duration("duration", 10*time.Minute, "measured soak duration")
		warmup            = fs.Duration("warmup", 0, "optional warmup duration discarded before the measured run")
		clients           = fs.Int("clients", 32, "concurrent HTTP clients")
		recordsPerRequest = fs.Int("records-per-request", 10, "records per HTTP produce request")
		payloadBytes      = fs.Int("payload-bytes", 512, "payload bytes per record")
		format            = fs.String("format", "json", "payload format: json or binary")
		timeout           = fs.Duration("timeout", 30*time.Second, "per-request timeout")
		maxLatencySamples = fs.Int("max-latency-samples", 1_000_000, "maximum latency samples retained for percentile calculation")
		maxFailureRate    = fs.String("max-failure-rate", "0", "maximum record failure rate as fraction or percent, for example 0.001 or 0.1%")
		minRecordsPerSec  = fs.Float64("min-records-sec", 0, "minimum successful records/sec; 0 disables this threshold")
		maxP99            = fs.Duration("max-p99", 0, "maximum p99 latency; 0 disables this threshold")
	)

	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}

	parsedURL, err := url.Parse(strings.TrimSpace(*rawURL))
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return options{}, fmt.Errorf("url must be an absolute URL")
	}
	if strings.TrimSpace(*topic) == "" {
		return options{}, fmt.Errorf("topic must not be empty")
	}
	if *duration <= 0 {
		return options{}, fmt.Errorf("duration must be positive")
	}
	if *warmup < 0 {
		return options{}, fmt.Errorf("warmup must be zero or positive")
	}
	if *clients <= 0 {
		return options{}, fmt.Errorf("clients must be positive")
	}
	if *recordsPerRequest <= 0 {
		return options{}, fmt.Errorf("records-per-request must be positive")
	}
	if *payloadBytes < 0 {
		return options{}, fmt.Errorf("payload-bytes must be zero or positive")
	}
	if *timeout <= 0 {
		return options{}, fmt.Errorf("timeout must be positive")
	}
	if *maxLatencySamples <= 0 {
		return options{}, fmt.Errorf("max-latency-samples must be positive")
	}
	if *minRecordsPerSec < 0 {
		return options{}, fmt.Errorf("min-records-sec must be zero or positive")
	}
	if *maxP99 < 0 {
		return options{}, fmt.Errorf("max-p99 must be zero or positive")
	}

	normalizedFormat, err := parseFormat(*format)
	if err != nil {
		return options{}, err
	}
	failureRate, err := parseFailureRate(*maxFailureRate)
	if err != nil {
		return options{}, fmt.Errorf("max-failure-rate: %w", err)
	}

	return options{
		URL:               strings.TrimRight(strings.TrimSpace(*rawURL), "/"),
		Topic:             strings.TrimSpace(*topic),
		Duration:          *duration,
		Warmup:            *warmup,
		Clients:           *clients,
		RecordsPerRequest: *recordsPerRequest,
		PayloadBytes:      *payloadBytes,
		Format:            normalizedFormat,
		Timeout:           *timeout,
		MaxLatencySamples: *maxLatencySamples,
		Thresholds: thresholds{
			MaxFailureRate:      failureRate,
			MinRecordsPerSecond: *minRecordsPerSec,
			MaxP99:              *maxP99,
		},
	}, nil
}

func runSoak(opts options, body []byte) summary {
	client, closeClient := newSoakHTTPClient(opts)
	defer closeClient()
	results := make(chan requestResult, opts.Clients*4)
	var wg sync.WaitGroup
	stop := time.NewTimer(opts.Duration)
	defer stop.Stop()
	done := make(chan struct{})
	start := time.Now()

	go func() {
		<-stop.C
		close(done)
	}()

	for i := 0; i < opts.Clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				results <- postOnce(client, opts, body)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	latencies := make([]time.Duration, 0, minInt(opts.MaxLatencySamples, 100_000))
	var totalRequests, successRequests, failedRequests int64
	var attemptedRecords, successRecords, failedRecords, requestBytes int64
	failureReasons := map[string]int64{}

	for res := range results {
		totalRequests++
		attemptedRecords += res.Records
		successRecords += res.SuccessRecords
		failedRecords += res.FailedRecords
		requestBytes += res.Bytes
		if res.Err != nil || res.Status < 200 || res.Status >= 300 || res.FailedRecords > 0 {
			failedRequests++
			reason := res.FailureReason
			if reason == "" {
				reason = "unknown_failure"
			}
			failureReasons[reason]++
		} else {
			successRequests++
		}
		if len(latencies) < opts.MaxLatencySamples {
			latencies = append(latencies, res.Latency)
		}
	}

	elapsed := time.Since(start)
	return summarize(opts, elapsed, totalRequests, successRequests, failedRequests, attemptedRecords, successRecords, failedRecords, requestBytes, latencies, failureReasons)
}

func newSoakHTTPClient(opts options) (*http.Client, func()) {
	maxConns := opts.Clients * 4
	if maxConns < 32 {
		maxConns = 32
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          maxConns,
		MaxIdleConnsPerHost:   maxConns,
		MaxConnsPerHost:       maxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: opts.Timeout,
		ExpectContinueTimeout: time.Second,
	}
	return &http.Client{Timeout: opts.Timeout, Transport: transport}, transport.CloseIdleConnections
}

func postOnce(client *http.Client, opts options, body []byte) requestResult {
	start := time.Now()
	u := opts.URL + "/topics/" + url.PathEscape(opts.Topic)
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return requestResult{Records: int64(opts.RecordsPerRequest), FailedRecords: int64(opts.RecordsPerRequest), Bytes: int64(len(body)), Latency: time.Since(start), Err: err, FailureReason: "request_build_error"}
	}
	req.Header.Set("Content-Type", contentTypeForFormat(opts.Format))
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := client.Do(req)
	if err != nil {
		return requestResult{Records: int64(opts.RecordsPerRequest), FailedRecords: int64(opts.RecordsPerRequest), Bytes: int64(len(body)), Latency: time.Since(start), Err: err, FailureReason: classifyClientError(err)}
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	latency := time.Since(start)
	if readErr != nil {
		return requestResult{Status: resp.StatusCode, Records: int64(opts.RecordsPerRequest), FailedRecords: int64(opts.RecordsPerRequest), Bytes: int64(len(body)), Latency: latency, Err: readErr, FailureReason: "response_read_error"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return requestResult{Status: resp.StatusCode, Records: int64(opts.RecordsPerRequest), FailedRecords: int64(opts.RecordsPerRequest), Bytes: int64(len(body)), Latency: latency, FailureReason: "http_status_" + strconv.Itoa(resp.StatusCode)}
	}

	success, failed, reason, err := countRecordResults(respBody, opts.RecordsPerRequest)
	if err != nil {
		return requestResult{Status: resp.StatusCode, Records: int64(opts.RecordsPerRequest), FailedRecords: int64(opts.RecordsPerRequest), Bytes: int64(len(body)), Latency: latency, Err: err, FailureReason: classifyResponseError(err)}
	}
	return requestResult{
		Status:         resp.StatusCode,
		Records:        int64(opts.RecordsPerRequest),
		SuccessRecords: int64(success),
		FailedRecords:  int64(failed),
		Bytes:          int64(len(body)),
		Latency:        latency,
		FailureReason:  reason,
	}
}

func countRecordResults(body []byte, expectedRecords int) (success int, failed int, failureReason string, err error) {
	var decoded produceResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return 0, 0, "", fmt.Errorf("decode produce response: %w", err)
	}
	if len(decoded.Offsets) != expectedRecords {
		return 0, 0, "", fmt.Errorf("produce response offsets length %d does not match records %d", len(decoded.Offsets), expectedRecords)
	}
	for _, offset := range decoded.Offsets {
		if offset.ErrorCode != nil || (offset.Error != nil && *offset.Error != "") {
			failed++
			if failureReason == "" {
				failureReason = recordFailureReason(offset)
			}
			continue
		}
		success++
	}
	return success, failed, failureReason, nil
}

func summarize(opts options, elapsed time.Duration, totalRequests, successRequests, failedRequests, attemptedRecords, successRecords, failedRecords, requestBytes int64, latencies []time.Duration, failureReasons map[string]int64) summary {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		elapsedSeconds = 0.001
	}

	var requestFailureRate float64
	if totalRequests > 0 {
		requestFailureRate = float64(failedRequests) / float64(totalRequests)
	}
	var recordFailureRate float64
	if attemptedRecords > 0 {
		recordFailureRate = float64(failedRecords) / float64(attemptedRecords)
	}

	return summary{
		URL:                opts.URL,
		Topic:              opts.Topic,
		Duration:           opts.Duration,
		Elapsed:            elapsed,
		Clients:            opts.Clients,
		RecordsPerRequest:  opts.RecordsPerRequest,
		PayloadBytes:       opts.PayloadBytes,
		Format:             opts.Format,
		TotalRequests:      totalRequests,
		SuccessRequests:    successRequests,
		FailedRequests:     failedRequests,
		AttemptedRecords:   attemptedRecords,
		SuccessRecords:     successRecords,
		FailedRecords:      failedRecords,
		RequestBytes:       requestBytes,
		RequestsPerSecond:  float64(totalRequests) / elapsedSeconds,
		RecordsPerSecond:   float64(successRecords) / elapsedSeconds,
		RequestFailureRate: requestFailureRate,
		RecordFailureRate:  recordFailureRate,
		P50:                percentile(latencies, 0.50),
		P95:                percentile(latencies, 0.95),
		P99:                percentile(latencies, 0.99),
		LatencySamples:     len(latencies),
		FailureBreakdown:   topFailureReasons(failureReasons, 8),
	}
}

func evaluateThresholds(result summary, limits thresholds) []violation {
	var violations []violation
	if result.RecordFailureRate > limits.MaxFailureRate {
		violations = append(violations, violation{
			Name:      "max_failure_rate",
			Actual:    formatPercent(result.RecordFailureRate),
			Threshold: "<= " + formatPercent(limits.MaxFailureRate),
		})
	}
	if limits.MinRecordsPerSecond > 0 && result.RecordsPerSecond < limits.MinRecordsPerSecond {
		violations = append(violations, violation{
			Name:      "min_records_sec",
			Actual:    fmt.Sprintf("%.2f", result.RecordsPerSecond),
			Threshold: ">= " + fmt.Sprintf("%.2f", limits.MinRecordsPerSecond),
		})
	}
	if limits.MaxP99 > 0 && result.P99 > limits.MaxP99 {
		violations = append(violations, violation{
			Name:      "max_p99",
			Actual:    result.P99.Round(time.Millisecond).String(),
			Threshold: "<= " + limits.MaxP99.String(),
		})
	}
	return violations
}

func printSummary(result summary, limits thresholds, violations []violation) {
	status := "pass"
	if len(violations) > 0 {
		status = "fail"
	}
	fmt.Printf("soak_result=%s url=%s topic=%s duration=%s elapsed=%s clients=%d records_per_request=%d payload_bytes=%d format=%s requests=%d success_requests=%d failed_requests=%d attempted_records=%d success_records=%d failed_records=%d records_per_sec=%.2f requests_per_sec=%.2f record_failure_rate=%s request_failure_rate=%s p50=%s p95=%s p99=%s latency_samples=%d thresholds=max_failure_rate:%s,min_records_sec:%.2f,max_p99:%s\n",
		status,
		result.URL,
		result.Topic,
		result.Duration,
		result.Elapsed.Round(time.Millisecond),
		result.Clients,
		result.RecordsPerRequest,
		result.PayloadBytes,
		result.Format,
		result.TotalRequests,
		result.SuccessRequests,
		result.FailedRequests,
		result.AttemptedRecords,
		result.SuccessRecords,
		result.FailedRecords,
		result.RecordsPerSecond,
		result.RequestsPerSecond,
		formatPercent(result.RecordFailureRate),
		formatPercent(result.RequestFailureRate),
		result.P50.Round(time.Millisecond),
		result.P95.Round(time.Millisecond),
		result.P99.Round(time.Millisecond),
		result.LatencySamples,
		formatPercent(limits.MaxFailureRate),
		limits.MinRecordsPerSecond,
		limits.MaxP99,
	)
	if len(result.FailureBreakdown) > 0 {
		fmt.Printf("failure_breakdown=%s\n", formatFailureBreakdown(result.FailureBreakdown))
	}
	for _, v := range violations {
		fmt.Printf("violation=%s actual=%s threshold=%s\n", v.Name, v.Actual, v.Threshold)
	}
}

func buildBody(records, payloadBytes int, format string) ([]byte, error) {
	payload := strings.Repeat("x", payloadBytes)
	req := produceRequest{Records: make([]produceRecord, records)}
	for i := 0; i < records; i++ {
		var value any
		if format == "binary" {
			value = base64.StdEncoding.EncodeToString([]byte(payload))
		} else {
			value = map[string]any{
				"payload": payload,
				"index":   i,
			}
		}
		req.Records[i] = produceRecord{Value: value}
	}
	return json.Marshal(req)
}

func classifyClientError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "client_timeout"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection reset"):
		return "client_connection_reset"
	case strings.Contains(msg, "connection refused"):
		return "client_connection_refused"
	case strings.Contains(msg, "eof"):
		return "client_eof"
	case strings.Contains(msg, "broken pipe"):
		return "client_broken_pipe"
	case strings.Contains(msg, "timeout"):
		return "client_timeout"
	default:
		return "client_error"
	}
}

func classifyResponseError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "decode produce response"):
		return "response_decode_error"
	case strings.Contains(msg, "offsets length"):
		return "response_offsets_length_mismatch"
	default:
		return "response_error"
	}
}

func recordFailureReason(offset produceOffset) string {
	if offset.ErrorCode != nil {
		return "record_error_code_" + strconv.Itoa(*offset.ErrorCode)
	}
	if offset.Error != nil && *offset.Error != "" {
		return "record_error"
	}
	return "record_error_unknown"
}

func topFailureReasons(reasons map[string]int64, limit int) []failureCount {
	if len(reasons) == 0 || limit <= 0 {
		return nil
	}
	out := make([]failureCount, 0, len(reasons))
	for reason, count := range reasons {
		out = append(out, failureCount{Reason: reason, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Reason < out[j].Reason
		}
		return out[i].Count > out[j].Count
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func formatFailureBreakdown(reasons []failureCount) string {
	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, reason.Reason+":"+strconv.FormatInt(reason.Count, 10))
	}
	return strings.Join(parts, ",")
}

func parseFormat(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "json":
		return "json", nil
	case "binary", "bin":
		return "binary", nil
	default:
		return "", fmt.Errorf("format must be json or binary, got %q", v)
	}
}

func contentTypeForFormat(format string) string {
	if format == "binary" {
		return "application/vnd.kafka.binary.v2+json"
	}
	return "application/vnd.kafka.json.v2+json"
}

func parseFailureRate(v string) (float64, error) {
	raw := strings.TrimSpace(v)
	if raw == "" {
		return 0, errors.New("must not be empty")
	}
	hasPercent := strings.HasSuffix(raw, "%")
	raw = strings.TrimSuffix(raw, "%")
	n, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", v)
	}
	if n < 0 {
		return 0, errors.New("must be zero or positive")
	}
	if hasPercent {
		n = n / 100
	}
	if n > 1 {
		return 0, errors.New("must be <= 1.0 as a fraction or <= 100%")
	}
	return n, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func formatPercent(v float64) string {
	return fmt.Sprintf("%.4f%%", v*100)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
