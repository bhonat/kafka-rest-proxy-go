package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type produceRequest struct {
	Records []produceRecord `json:"records"`
}

type produceRecord struct {
	Key   *string `json:"key,omitempty"`
	Value any     `json:"value"`
}

type targetFlags []targetSpec

type targetSpec struct {
	Name string
	URL  string
}

type scenarioSpec struct {
	PayloadBytes      int
	RecordsPerRequest int
	Clients           int
	Format            string
	Compression       string
	Acks              string
}

type result struct {
	status  int
	records int64
	bytes   int64
	latency time.Duration
	err     error
}

type benchOptions struct {
	Topic       string
	Duration    time.Duration
	Requests    int64
	Timeout     time.Duration
	MaxSamples  int
	KeyPrefix   string
	Targets     []targetSpec
	Scenarios   []scenarioSpec
	HTMLPath    string
	Suite       bool
	GeneratedAt time.Time
	Capacity    capacityConfig
}

type capacityConfig struct {
	TargetRecordsPerSecond float64
	Headroom               float64
}

type benchResult struct {
	GeneratedAt       string
	TargetName        string
	TargetURL         string
	Scenario          string
	Topic             string
	Duration          string
	RequestsLimit     int64
	Timeout           string
	KeyPrefix         string
	PayloadBytes      int
	RecordsPerRequest int
	Clients           int
	Format            string
	Compression       string
	Acks              string
	Elapsed           string
	TotalRequests     int64
	SuccessRequests   int64
	FailedRequests    int64
	AttemptedRecords  int64
	SuccessRecords    int64
	RequestBytes      int64
	RequestsPerSecond float64
	RecordsPerSecond  float64
	MiBPerSecond      float64
	FailureRate       float64
	CapacityNodes     int
	LatencyP50        time.Duration
	LatencyP95        time.Duration
	LatencyP99        time.Duration
	LatencySamples    int
	Error             string

	RequestsPerSecondText string
	RecordsPerSecondText  string
	MiBPerSecondText      string
	FailureRateText       string
	CapacityNodesText     string
	LatencyP50Text        string
	LatencyP95Text        string
	LatencyP99Text        string
	LatencyP50Millis      string
	LatencyP95Millis      string
	LatencyP99Millis      string
}

type benchReport struct {
	GeneratedAt string
	Suite       bool
	Targets     []targetSpec
	Results     []benchResult
	Comparisons []comparisonRow
	Capacity    capacityReport
}

type capacityReport struct {
	Enabled       bool
	TargetText    string
	HeadroomText  string
	EffectiveText string
	Description   string
}

type comparisonRow struct {
	Scenario                 string
	Winner                   string
	BestRecordsPerSecondText string
	NodeWinner               string
	FewestNodesText          string
	Results                  []benchResult
}

func main() {
	var targets targetFlags
	var (
		baseURL           = flag.String("url", "http://localhost:8080", "REST proxy base URL used when -target is not provided")
		confluentURL      = flag.String("confluent-url", "", "optional Confluent REST Proxy URL; adds target name 'confluent'")
		topic             = flag.String("topic", "orders", "Kafka topic")
		duration          = flag.Duration("duration", 30*time.Second, "benchmark duration per target/scenario")
		clients           = flag.Int("clients", 32, "concurrent HTTP clients for single-scenario mode")
		records           = flag.Int("records", 10, "records per HTTP request for single-scenario mode")
		payloadBytes      = flag.Int("payload-bytes", 512, "payload string bytes per record for single-scenario mode")
		requests          = flag.Int64("requests", 0, "optional max HTTP requests per target/scenario; 0 means duration-based")
		timeout           = flag.Duration("timeout", 30*time.Second, "per-request timeout")
		maxSamples        = flag.Int("max-latency-samples", 1_000_000, "max latency samples retained per target/scenario")
		keyPrefix         = flag.String("key-prefix", "", "optional static key prefix; empty omits keys for partition spreading")
		format            = flag.String("format", "json", "payload format for single-scenario mode: json or binary")
		htmlPath          = flag.String("html", "", "optional path for standalone HTML benchmark report")
		suite             = flag.Bool("suite", false, "run the multi-scenario benchmark suite")
		payloadSizes      = flag.String("payload-sizes", "128,512,1KB,10KB", "comma-separated payload sizes for suite mode; supports B, KB, KiB, MB, MiB")
		recordsPerRequest = flag.String("records-per-request", "1,10,100,1000", "comma-separated records/request values for suite mode")
		clientCounts      = flag.String("client-counts", "4,16,64,256", "comma-separated concurrent client counts for suite mode")
		formats           = flag.String("formats", "json", "comma-separated payload formats for suite mode: json,binary")
		compressionLabels = flag.String("compression-labels", "runtime", "comma-separated suite labels for externally configured Kafka compression variants")
		acksLabels        = flag.String("acks-labels", "runtime", "comma-separated suite labels for externally configured Kafka required-acks variants")
		compressionLabel  = flag.String("compression-label", "runtime", "single-scenario label for externally configured Kafka compression")
		acksLabel         = flag.String("acks-label", "runtime", "single-scenario label for externally configured Kafka required-acks")
		capacityTarget    = flag.Float64("capacity-target-records", 1_000_000, "target records/sec for node-count estimates; set 0 to disable")
		capacityHeadroom  = flag.Float64("capacity-headroom", 0.30, "extra headroom fraction for node-count estimates, for example 0.30 means 30%")
	)
	flag.Var(&targets, "target", "benchmark target in name=url form; repeat for comparison, for example -target go=http://localhost:8080 -target confluent=http://localhost:8082")
	flag.Parse()

	opts, err := buildOptions(*baseURL, *confluentURL, targets, *topic, *duration, *requests, *timeout, *maxSamples, *keyPrefix, *htmlPath, *suite, *payloadBytes, *records, *clients, *format, *payloadSizes, *recordsPerRequest, *clientCounts, *formats, *compressionLabels, *acksLabels, *compressionLabel, *acksLabel, *capacityTarget, *capacityHeadroom)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	report := benchReport{
		GeneratedAt: opts.GeneratedAt.Format(time.RFC3339),
		Suite:       opts.Suite,
		Targets:     opts.Targets,
		Capacity:    capacityReportFromConfig(opts.Capacity),
	}

	for _, scenario := range opts.Scenarios {
		for _, target := range opts.Targets {
			res := runScenario(opts, target, scenario)
			report.Results = append(report.Results, res)
			printResult(res)
		}
	}

	report.Comparisons = buildComparisons(report.Results)

	if opts.HTMLPath != "" {
		if err := writeHTMLReport(opts.HTMLPath, report); err != nil {
			fmt.Fprintf(os.Stderr, "write html report: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("html_report=%s\n", opts.HTMLPath)
	}
}

func buildOptions(baseURL, confluentURL string, targets targetFlags, topic string, duration time.Duration, requests int64, timeout time.Duration, maxSamples int, keyPrefix, htmlPath string, suite bool, payloadBytes, records, clients int, format, payloadSizes, recordsPerRequest, clientCounts, formats, compressionLabels, acksLabels, compressionLabel, acksLabel string, capacityTarget, capacityHeadroom float64) (benchOptions, error) {
	if strings.TrimSpace(topic) == "" {
		return benchOptions{}, fmt.Errorf("topic must not be empty")
	}
	if duration <= 0 || timeout <= 0 {
		return benchOptions{}, fmt.Errorf("duration and timeout must be positive")
	}
	if maxSamples <= 0 {
		return benchOptions{}, fmt.Errorf("max-latency-samples must be positive")
	}
	if capacityTarget < 0 {
		return benchOptions{}, fmt.Errorf("capacity-target-records must be zero or positive")
	}
	if capacityHeadroom < 0 {
		return benchOptions{}, fmt.Errorf("capacity-headroom must be zero or positive")
	}

	targetsOut := append([]targetSpec(nil), targets...)
	if len(targetsOut) == 0 {
		t, err := parseTarget("go=" + baseURL)
		if err != nil {
			return benchOptions{}, err
		}
		targetsOut = append(targetsOut, t)
	}
	if strings.TrimSpace(confluentURL) != "" {
		t, err := parseTarget("confluent=" + confluentURL)
		if err != nil {
			return benchOptions{}, err
		}
		targetsOut = append(targetsOut, t)
	}

	var scenarios []scenarioSpec
	if suite {
		payloads, err := parseByteSizeList(payloadSizes)
		if err != nil {
			return benchOptions{}, fmt.Errorf("payload-sizes: %w", err)
		}
		recordCounts, err := parseIntList(recordsPerRequest)
		if err != nil {
			return benchOptions{}, fmt.Errorf("records-per-request: %w", err)
		}
		clientsList, err := parseIntList(clientCounts)
		if err != nil {
			return benchOptions{}, fmt.Errorf("client-counts: %w", err)
		}
		formatValues, err := parseFormatList(formats)
		if err != nil {
			return benchOptions{}, fmt.Errorf("formats: %w", err)
		}
		compressions := parseLabelList(compressionLabels)
		acksValues := parseLabelList(acksLabels)
		for _, payload := range payloads {
			for _, recs := range recordCounts {
				for _, clientCount := range clientsList {
					for _, format := range formatValues {
						for _, compression := range compressions {
							for _, acks := range acksValues {
								scenarios = append(scenarios, scenarioSpec{
									PayloadBytes:      payload,
									RecordsPerRequest: recs,
									Clients:           clientCount,
									Format:            format,
									Compression:       compression,
									Acks:              acks,
								})
							}
						}
					}
				}
			}
		}
	} else {
		if clients <= 0 || records <= 0 || payloadBytes < 0 {
			return benchOptions{}, fmt.Errorf("clients and records must be positive, and payload-bytes must be zero or positive")
		}
		formatValue, err := parseFormat(format)
		if err != nil {
			return benchOptions{}, err
		}
		scenarios = []scenarioSpec{{
			PayloadBytes:      payloadBytes,
			RecordsPerRequest: records,
			Clients:           clients,
			Format:            formatValue,
			Compression:       firstLabel(compressionLabel),
			Acks:              firstLabel(acksLabel),
		}}
	}

	if len(scenarios) == 0 {
		return benchOptions{}, fmt.Errorf("at least one scenario is required")
	}

	return benchOptions{
		Topic:       topic,
		Duration:    duration,
		Requests:    requests,
		Timeout:     timeout,
		MaxSamples:  maxSamples,
		KeyPrefix:   keyPrefix,
		Targets:     targetsOut,
		Scenarios:   scenarios,
		HTMLPath:    htmlPath,
		Suite:       suite,
		GeneratedAt: time.Now(),
		Capacity: capacityConfig{
			TargetRecordsPerSecond: capacityTarget,
			Headroom:               capacityHeadroom,
		},
	}, nil
}

func runScenario(opts benchOptions, target targetSpec, scenario scenarioSpec) benchResult {
	body, err := buildBody(scenario.RecordsPerRequest, scenario.PayloadBytes, opts.KeyPrefix, scenario.Format)
	if err != nil {
		return failedResult(opts, target, scenario, err)
	}

	results := make(chan result, scenario.Clients*4)
	var started atomic.Int64
	var wg sync.WaitGroup
	stop := time.NewTimer(opts.Duration)
	defer stop.Stop()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		<-stop.C
		close(done)
	}()

	for i := 0; i < scenario.Clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: opts.Timeout}
			for {
				if opts.Requests > 0 && started.Add(1) > opts.Requests {
					return
				}
				select {
				case <-done:
					return
				default:
				}
				results <- postOnce(client, target.URL, opts.Topic, body, int64(scenario.RecordsPerRequest), opts.Timeout, scenario.Format)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalRequests, successRequests, failedRequests, attemptedRecords, successRecords, requestBytes int64
	latencies := make([]time.Duration, 0, min(opts.MaxSamples, 100_000))
	for res := range results {
		totalRequests++
		attemptedRecords += res.records
		requestBytes += res.bytes
		if res.err != nil || res.status < 200 || res.status >= 300 {
			failedRequests++
		} else {
			successRequests++
			successRecords += res.records
		}
		if len(latencies) < opts.MaxSamples {
			latencies = append(latencies, res.latency)
		}
	}

	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return summarize(opts, target, scenario, elapsed, totalRequests, successRequests, failedRequests, attemptedRecords, successRecords, requestBytes, latencies, "")
}

func failedResult(opts benchOptions, target targetSpec, scenario scenarioSpec, err error) benchResult {
	return summarize(opts, target, scenario, 0, 0, 0, 0, 0, 0, 0, nil, err.Error())
}

func buildBody(records, payloadBytes int, keyPrefix, format string) ([]byte, error) {
	payload := strings.Repeat("x", payloadBytes)
	req := produceRequest{Records: make([]produceRecord, records)}
	for i := 0; i < records; i++ {
		var key *string
		if keyPrefix != "" {
			k := fmt.Sprintf("%s-%d", keyPrefix, i)
			if format == "binary" {
				k = base64.StdEncoding.EncodeToString([]byte(k))
			}
			key = &k
		}
		var value any
		if format == "binary" {
			value = base64.StdEncoding.EncodeToString([]byte(payload))
		} else {
			value = map[string]any{
				"payload": payload,
				"index":   i,
			}
		}
		req.Records[i] = produceRecord{
			Key:   key,
			Value: value,
		}
	}
	return json.Marshal(req)
}

func postOnce(client *http.Client, baseURL, topic string, body []byte, records int64, timeout time.Duration, format string) result {
	start := time.Now()
	u := strings.TrimRight(baseURL, "/") + "/topics/" + url.PathEscape(topic)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return result{err: err, latency: time.Since(start)}
	}
	req.Header.Set("Content-Type", contentTypeForFormat(format))
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := client.Do(req)
	if err != nil {
		return result{err: err, latency: time.Since(start), bytes: int64(len(body))}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return result{
		status:  resp.StatusCode,
		records: records,
		bytes:   int64(len(body)),
		latency: time.Since(start),
	}
}

func summarize(opts benchOptions, target targetSpec, scenario scenarioSpec, elapsed time.Duration, totalRequests, successRequests, failedRequests, attemptedRecords, successRecords, requestBytes int64, latencies []time.Duration, errText string) benchResult {
	elapsedSeconds := max(elapsed.Seconds(), 0.001)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	var failureRate float64
	if totalRequests > 0 {
		failureRate = float64(failedRequests) / float64(totalRequests) * 100
	}

	rps := float64(totalRequests) / elapsedSeconds
	recordsPerSec := float64(successRecords) / elapsedSeconds
	mibPerSec := float64(requestBytes) / (1024 * 1024) / elapsedSeconds
	capacityNodes, capacityNodesText := estimateNodes(opts.Capacity, recordsPerSec)

	res := benchResult{
		GeneratedAt:       opts.GeneratedAt.Format(time.RFC3339),
		TargetName:        target.Name,
		TargetURL:         target.URL,
		Scenario:          scenarioLabel(scenario),
		Topic:             opts.Topic,
		Duration:          opts.Duration.String(),
		RequestsLimit:     opts.Requests,
		Timeout:           opts.Timeout.String(),
		KeyPrefix:         opts.KeyPrefix,
		PayloadBytes:      scenario.PayloadBytes,
		RecordsPerRequest: scenario.RecordsPerRequest,
		Clients:           scenario.Clients,
		Format:            scenario.Format,
		Compression:       scenario.Compression,
		Acks:              scenario.Acks,
		Elapsed:           elapsed.Round(time.Millisecond).String(),
		TotalRequests:     totalRequests,
		SuccessRequests:   successRequests,
		FailedRequests:    failedRequests,
		AttemptedRecords:  attemptedRecords,
		SuccessRecords:    successRecords,
		RequestBytes:      requestBytes,
		RequestsPerSecond: rps,
		RecordsPerSecond:  recordsPerSec,
		MiBPerSecond:      mibPerSec,
		FailureRate:       failureRate,
		CapacityNodes:     capacityNodes,
		LatencyP50:        p50,
		LatencyP95:        p95,
		LatencyP99:        p99,
		LatencySamples:    len(latencies),
		Error:             errText,
	}
	res.RequestsPerSecondText = fmt.Sprintf("%.2f", res.RequestsPerSecond)
	res.RecordsPerSecondText = fmt.Sprintf("%.2f", res.RecordsPerSecond)
	res.MiBPerSecondText = fmt.Sprintf("%.2f", res.MiBPerSecond)
	res.FailureRateText = fmt.Sprintf("%.2f%%", res.FailureRate)
	res.CapacityNodesText = capacityNodesText
	res.LatencyP50Text = p50.Round(time.Millisecond).String()
	res.LatencyP95Text = p95.Round(time.Millisecond).String()
	res.LatencyP99Text = p99.Round(time.Millisecond).String()
	res.LatencyP50Millis = fmt.Sprintf("%.2f", float64(p50)/float64(time.Millisecond))
	res.LatencyP95Millis = fmt.Sprintf("%.2f", float64(p95)/float64(time.Millisecond))
	res.LatencyP99Millis = fmt.Sprintf("%.2f", float64(p99)/float64(time.Millisecond))
	return res
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

func printResult(res benchResult) {
	fmt.Printf("target=%s scenario=%q elapsed=%s requests=%d success=%d failed=%d records=%d records_per_sec=%s requests_per_sec=%s capacity_nodes=%s latency_p50=%s latency_p95=%s latency_p99=%s failure_rate=%s\n",
		res.TargetName,
		res.Scenario,
		res.Elapsed,
		res.TotalRequests,
		res.SuccessRequests,
		res.FailedRequests,
		res.SuccessRecords,
		res.RecordsPerSecondText,
		res.RequestsPerSecondText,
		res.CapacityNodesText,
		res.LatencyP50Text,
		res.LatencyP95Text,
		res.LatencyP99Text,
		res.FailureRateText,
	)
	if res.Error != "" {
		fmt.Printf("target=%s scenario=%q error=%s\n", res.TargetName, res.Scenario, res.Error)
	}
}

func estimateNodes(cfg capacityConfig, recordsPerSecond float64) (int, string) {
	if cfg.TargetRecordsPerSecond <= 0 {
		return 0, "n/a"
	}
	if recordsPerSecond <= 0 {
		return 0, "∞"
	}
	effectiveTarget := cfg.TargetRecordsPerSecond * (1 + cfg.Headroom)
	nodes := int(math.Ceil(effectiveTarget / recordsPerSecond))
	if nodes < 1 {
		nodes = 1
	}
	return nodes, strconv.Itoa(nodes)
}

func capacityReportFromConfig(cfg capacityConfig) capacityReport {
	if cfg.TargetRecordsPerSecond <= 0 {
		return capacityReport{}
	}
	effectiveTarget := cfg.TargetRecordsPerSecond * (1 + cfg.Headroom)
	return capacityReport{
		Enabled:       true,
		TargetText:    formatRate(cfg.TargetRecordsPerSecond),
		HeadroomText:  fmt.Sprintf("%.0f%%", cfg.Headroom*100),
		EffectiveText: formatRate(effectiveTarget),
		Description:   "Nodes are estimated as ceil(target records/sec × (1 + headroom) ÷ measured records/sec). Treat rows with non-zero failure rate as saturation signals, not reliable sizing points.",
	}
}

func formatRate(v float64) string {
	switch {
	case v >= 1_000_000:
		return fmt.Sprintf("%.2fM", v/1_000_000)
	case v >= 1_000:
		return fmt.Sprintf("%.2fK", v/1_000)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func (t *targetFlags) Set(v string) error {
	target, err := parseTarget(v)
	if err != nil {
		return err
	}
	*t = append(*t, target)
	return nil
}

func (t *targetFlags) String() string {
	if t == nil || len(*t) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*t))
	for _, target := range *t {
		parts = append(parts, target.Name+"="+target.URL)
	}
	return strings.Join(parts, ",")
}

func parseTarget(v string) (targetSpec, error) {
	name, rawURL, ok := strings.Cut(v, "=")
	if !ok {
		name = fmt.Sprintf("target%d", time.Now().UnixNano())
		rawURL = v
	}
	name = strings.TrimSpace(name)
	rawURL = strings.TrimSpace(rawURL)
	if name == "" || rawURL == "" {
		return targetSpec{}, fmt.Errorf("target must be name=url")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return targetSpec{}, fmt.Errorf("target %q must be an absolute URL", v)
	}
	return targetSpec{Name: name, URL: strings.TrimRight(rawURL, "/")}, nil
}

func parseByteSizeList(v string) ([]int, error) {
	parts := splitCSV(v)
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := parseByteSize(part)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return positiveInts(out)
}

func parseIntList(v string) ([]int, error) {
	parts := splitCSV(v)
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", part)
		}
		out = append(out, n)
	}
	return positiveInts(out)
}

func positiveInts(in []int) ([]int, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("at least one value is required")
	}
	for _, n := range in {
		if n <= 0 {
			return nil, fmt.Errorf("values must be positive")
		}
	}
	return in, nil
}

func parseByteSize(v string) (int, error) {
	raw := strings.ToUpper(strings.TrimSpace(v))
	raw = strings.ReplaceAll(raw, " ", "")
	if raw == "" {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := 1
	switch {
	case strings.HasSuffix(raw, "KIB"):
		multiplier = 1024
		raw = strings.TrimSuffix(raw, "KIB")
	case strings.HasSuffix(raw, "KB"):
		multiplier = 1000
		raw = strings.TrimSuffix(raw, "KB")
	case strings.HasSuffix(raw, "MIB"):
		multiplier = 1024 * 1024
		raw = strings.TrimSuffix(raw, "MIB")
	case strings.HasSuffix(raw, "MB"):
		multiplier = 1000 * 1000
		raw = strings.TrimSuffix(raw, "MB")
	case strings.HasSuffix(raw, "B"):
		raw = strings.TrimSuffix(raw, "B")
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid byte size", v)
	}
	return n * multiplier, nil
}

func parseLabelList(v string) []string {
	parts := splitCSV(v)
	if len(parts) == 0 {
		return []string{"runtime"}
	}
	return parts
}

func parseFormatList(v string) ([]string, error) {
	parts := splitCSV(v)
	if len(parts) == 0 {
		return []string{"json"}, nil
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		format, err := parseFormat(part)
		if err != nil {
			return nil, err
		}
		out = append(out, format)
	}
	return out, nil
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

func firstLabel(v string) string {
	labels := parseLabelList(v)
	return labels[0]
}

func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func scenarioLabel(s scenarioSpec) string {
	return fmt.Sprintf("format=%s payload=%s records/request=%d clients=%d compression=%s acks=%s",
		s.Format,
		formatBytes(s.PayloadBytes),
		s.RecordsPerRequest,
		s.Clients,
		s.Compression,
		s.Acks,
	)
}

func formatBytes(n int) string {
	switch {
	case n%1024 == 0 && n >= 1024*1024:
		return fmt.Sprintf("%dMiB", n/(1024*1024))
	case n%1024 == 0 && n >= 1024:
		return fmt.Sprintf("%dKiB", n/1024)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func buildComparisons(results []benchResult) []comparisonRow {
	grouped := make(map[string][]benchResult)
	for _, res := range results {
		grouped[res.Scenario] = append(grouped[res.Scenario], res)
	}
	scenarios := make([]string, 0, len(grouped))
	for scenario := range grouped {
		scenarios = append(scenarios, scenario)
	}
	sort.Strings(scenarios)

	rows := make([]comparisonRow, 0, len(scenarios))
	for _, scenario := range scenarios {
		group := grouped[scenario]
		sort.Slice(group, func(i, j int) bool {
			return group[i].TargetName < group[j].TargetName
		})
		winner := ""
		nodeWinner := ""
		var best float64
		var fewestNodes int
		for _, res := range group {
			if winner == "" || res.RecordsPerSecond > best {
				winner = res.TargetName
				best = res.RecordsPerSecond
			}
			if res.CapacityNodes > 0 && (nodeWinner == "" || res.CapacityNodes < fewestNodes) {
				nodeWinner = res.TargetName
				fewestNodes = res.CapacityNodes
			}
		}
		fewestNodesText := "n/a"
		if nodeWinner != "" {
			fewestNodesText = strconv.Itoa(fewestNodes)
		}
		rows = append(rows, comparisonRow{
			Scenario:                 scenario,
			Winner:                   winner,
			BestRecordsPerSecondText: fmt.Sprintf("%.2f", best),
			NodeWinner:               nodeWinner,
			FewestNodesText:          fewestNodesText,
			Results:                  group,
		})
	}
	return rows
}

func writeHTMLReport(path string, report benchReport) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return htmlReportTemplate.Execute(f, report)
}

var htmlReportTemplate = template.Must(template.New("html-report").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kafka REST Proxy Benchmark Report</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: Canvas;
      --fg: CanvasText;
      --muted: color-mix(in srgb, CanvasText 65%, Canvas);
      --card: color-mix(in srgb, Canvas 92%, CanvasText);
      --border: color-mix(in srgb, CanvasText 18%, Canvas);
      --accent: #3b82f6;
      --good: #16a34a;
      --bad: #dc2626;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--fg);
      line-height: 1.45;
    }
    main {
      width: min(1280px, calc(100vw - 32px));
      margin: 32px auto;
    }
    h1 { margin: 0 0 6px; font-size: clamp(1.6rem, 2vw, 2.2rem); }
    h2 { margin: 28px 0 12px; font-size: 1.1rem; }
    .muted { color: var(--muted); }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 24px 0;
    }
    .card {
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      background: var(--card);
    }
    .label {
      color: var(--muted);
      font-size: .85rem;
      margin-bottom: 6px;
    }
    .value {
      font-size: 1.45rem;
      font-weight: 650;
      letter-spacing: -.02em;
    }
    .ok { color: var(--good); }
    .bad { color: var(--bad); }
    table {
      width: 100%;
      border-collapse: collapse;
      border: 1px solid var(--border);
      border-radius: 14px;
      overflow: hidden;
      margin-bottom: 20px;
    }
    th, td {
      text-align: left;
      padding: 9px 10px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
      white-space: nowrap;
    }
    th { color: var(--muted); font-weight: 600; }
    tr:last-child th, tr:last-child td { border-bottom: 0; }
    .scenario { white-space: normal; min-width: 280px; }
    .target { font-weight: 650; }
    code {
      background: color-mix(in srgb, CanvasText 8%, Canvas);
      padding: 2px 5px;
      border-radius: 5px;
    }
    .scroll { overflow-x: auto; }
    @media (max-width: 760px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 460px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
<main>
  <h1>Kafka REST Proxy Benchmark Report</h1>
  <div class="muted">Generated {{ .GeneratedAt }}. {{ if .Suite }}Suite mode{{ else }}Single scenario{{ end }} across {{ len .Targets }} target(s).</div>
  {{ if .Capacity.Enabled }}
  <p class="muted">
    Capacity target: <strong>{{ .Capacity.TargetText }} records/sec</strong> with
    <strong>{{ .Capacity.HeadroomText }}</strong> headroom
    (effective target {{ .Capacity.EffectiveText }} records/sec). {{ .Capacity.Description }}
  </p>
  {{ end }}

  {{ with index .Results 0 }}
  <section class="grid" aria-label="First benchmark summary">
    <div class="card"><div class="label">First result records/sec</div><div class="value">{{ .RecordsPerSecondText }}</div></div>
    <div class="card"><div class="label">First result requests/sec</div><div class="value">{{ .RequestsPerSecondText }}</div></div>
    <div class="card"><div class="label">First result p99 latency</div><div class="value">{{ .LatencyP99Text }}</div></div>
    {{ if $.Capacity.Enabled }}
    <div class="card"><div class="label">First result estimated nodes</div><div class="value">{{ .CapacityNodesText }}</div></div>
    {{ else }}
    <div class="card"><div class="label">First result failure rate</div><div class="value {{ if eq .FailedRequests 0 }}ok{{ else }}bad{{ end }}">{{ .FailureRateText }}</div></div>
    {{ end }}
  </section>
  {{ end }}

  <h2>Targets</h2>
  <div class="scroll">
    <table>
      <thead><tr><th>Name</th><th>URL</th></tr></thead>
      <tbody>
        {{ range .Targets }}
        <tr><td class="target">{{ .Name }}</td><td><code>{{ .URL }}</code></td></tr>
        {{ end }}
      </tbody>
    </table>
  </div>

  {{ if gt (len .Targets) 1 }}
  <h2>Scenario winners</h2>
  <div class="scroll">
    <table>
      <thead><tr><th>Scenario</th><th>Throughput winner</th><th>Best records/sec</th>{{ if $.Capacity.Enabled }}<th>Fewest-node winner</th><th>Estimated nodes</th>{{ end }}</tr></thead>
      <tbody>
        {{ range .Comparisons }}
        <tr>
          <td class="scenario">{{ .Scenario }}</td>
          <td class="target">{{ .Winner }}</td>
          <td>{{ .BestRecordsPerSecondText }}</td>
          {{ if $.Capacity.Enabled }}<td class="target">{{ .NodeWinner }}</td><td>{{ .FewestNodesText }}</td>{{ end }}
        </tr>
        {{ end }}
      </tbody>
    </table>
  </div>
  {{ end }}

  <h2>All results</h2>
  <div class="scroll">
    <table>
      <thead>
        <tr>
          <th>Target</th>
          <th>Scenario</th>
          <th>Format</th>
          <th>Records/sec</th>
          <th>Requests/sec</th>
          {{ if $.Capacity.Enabled }}<th>Nodes for target</th>{{ end }}
          <th>p50</th>
          <th>p95</th>
          <th>p99</th>
          <th>Failure rate</th>
          <th>Success records</th>
          <th>Requests</th>
          <th>MiB/sec</th>
          <th>Elapsed</th>
        </tr>
      </thead>
      <tbody>
        {{ range .Results }}
        <tr>
          <td class="target">{{ .TargetName }}</td>
          <td class="scenario">{{ .Scenario }}</td>
          <td>{{ .Format }}</td>
          <td>{{ .RecordsPerSecondText }}</td>
          <td>{{ .RequestsPerSecondText }}</td>
          {{ if $.Capacity.Enabled }}<td>{{ .CapacityNodesText }}</td>{{ end }}
          <td>{{ .LatencyP50Text }}</td>
          <td>{{ .LatencyP95Text }}</td>
          <td>{{ .LatencyP99Text }}</td>
          <td class="{{ if eq .FailedRequests 0 }}ok{{ else }}bad{{ end }}">{{ .FailureRateText }}</td>
          <td>{{ .SuccessRecords }}</td>
          <td>{{ .SuccessRequests }} / {{ .TotalRequests }}</td>
          <td>{{ .MiBPerSecondText }}</td>
          <td>{{ .Elapsed }}</td>
        </tr>
        {{ end }}
      </tbody>
    </table>
  </div>

  <p class="muted">
    Compression and acks values are benchmark labels. Configure each target process with the desired Kafka producer settings,
    then pass each configured process as a separate <code>-target name=url</code> for apples-to-apples comparison.
  </p>
</main>
</body>
</html>
`))
