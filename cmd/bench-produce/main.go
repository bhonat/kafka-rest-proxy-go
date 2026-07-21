package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type produceRequest struct {
	Records []produceRecord `json:"records"`
}

type produceRecord struct {
	Key   *string        `json:"key,omitempty"`
	Value map[string]any `json:"value"`
}

type result struct {
	status  int
	records int64
	bytes   int64
	latency time.Duration
	err     error
}

type benchConfig struct {
	URL          string
	Topic        string
	Duration     string
	Clients      int
	Records      int
	PayloadBytes int
	Requests     int64
	Timeout      string
	KeyPrefix    string
}

type benchSummary struct {
	GeneratedAt       string
	Config            benchConfig
	Elapsed           string
	TotalRequests     int64
	SuccessRequests   int64
	FailedRequests    int64
	TotalRecords      int64
	TotalBytes        int64
	RequestsPerSecond string
	RecordsPerSecond  string
	MiBPerSecond      string
	FailureRate       string
	LatencyP50        string
	LatencyP95        string
	LatencyP99        string
	LatencySamples    int
	LatencyP50Millis  string
	LatencyP95Millis  string
	LatencyP99Millis  string
}

func main() {
	var (
		baseURL      = flag.String("url", "http://localhost:8080", "REST proxy base URL")
		topic        = flag.String("topic", "orders", "Kafka topic")
		duration     = flag.Duration("duration", 30*time.Second, "benchmark duration")
		clients      = flag.Int("clients", 32, "concurrent HTTP clients")
		records      = flag.Int("records", 10, "records per HTTP request")
		payloadBytes = flag.Int("payload-bytes", 512, "payload string bytes per record")
		requests     = flag.Int64("requests", 0, "optional max HTTP requests; 0 means duration-based")
		timeout      = flag.Duration("timeout", 30*time.Second, "per-request timeout")
		maxSamples   = flag.Int("max-latency-samples", 1_000_000, "max latency samples retained for percentiles")
		keyPrefix    = flag.String("key-prefix", "", "optional static key prefix; empty omits keys for partition spreading")
		htmlPath     = flag.String("html", "", "optional path for standalone HTML benchmark report")
	)
	flag.Parse()

	if *clients <= 0 || *records <= 0 || *payloadBytes < 0 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "clients, records, payload-bytes, and duration must be valid positive values")
		os.Exit(2)
	}

	body, err := buildBody(*records, *payloadBytes, *keyPrefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	results := make(chan result, *clients*4)
	var started atomic.Int64
	var wg sync.WaitGroup
	stop := time.NewTimer(*duration)
	defer stop.Stop()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		<-stop.C
		close(done)
	}()

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: *timeout}
			for {
				if *requests > 0 && started.Add(1) > *requests {
					return
				}
				select {
				case <-done:
					return
				default:
				}
				results <- postOnce(client, *baseURL, *topic, body, int64(*records), *timeout)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalRequests, successRequests, failedRequests, totalRecords, totalBytes int64
	latencies := make([]time.Duration, 0, min(*maxSamples, 100_000))
	for res := range results {
		totalRequests++
		totalRecords += res.records
		totalBytes += res.bytes
		if res.err != nil || res.status < 200 || res.status >= 300 {
			failedRequests++
		} else {
			successRequests++
		}
		if len(latencies) < *maxSamples {
			latencies = append(latencies, res.latency)
		}
	}

	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	summary := summarize(benchConfig{
		URL:          *baseURL,
		Topic:        *topic,
		Duration:     duration.String(),
		Clients:      *clients,
		Records:      *records,
		PayloadBytes: *payloadBytes,
		Requests:     *requests,
		Timeout:      timeout.String(),
		KeyPrefix:    *keyPrefix,
	}, elapsed, totalRequests, successRequests, failedRequests, totalRecords, totalBytes, latencies)

	printSummary(summary)
	if *htmlPath != "" {
		if err := writeHTMLReport(*htmlPath, summary); err != nil {
			fmt.Fprintf(os.Stderr, "write html report: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("html_report=%s\n", *htmlPath)
	}
}

func buildBody(records, payloadBytes int, keyPrefix string) ([]byte, error) {
	payload := strings.Repeat("x", payloadBytes)
	req := produceRequest{Records: make([]produceRecord, records)}
	for i := 0; i < records; i++ {
		var key *string
		if keyPrefix != "" {
			k := fmt.Sprintf("%s-%d", keyPrefix, i)
			key = &k
		}
		req.Records[i] = produceRecord{
			Key: key,
			Value: map[string]any{
				"payload": payload,
				"index":   i,
			},
		}
	}
	return json.Marshal(req)
}

func postOnce(client *http.Client, baseURL, topic string, body []byte, records int64, timeout time.Duration) result {
	start := time.Now()
	url := strings.TrimRight(baseURL, "/") + "/topics/" + topic
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return result{err: err, latency: time.Since(start)}
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
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

func summarize(cfg benchConfig, elapsed time.Duration, totalRequests, successRequests, failedRequests, totalRecords, totalBytes int64, latencies []time.Duration) benchSummary {
	elapsedSeconds := max(elapsed.Seconds(), 0.001)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	var failureRate float64
	if totalRequests > 0 {
		failureRate = float64(failedRequests) / float64(totalRequests) * 100
	}

	return benchSummary{
		GeneratedAt:       time.Now().Format(time.RFC3339),
		Config:            cfg,
		Elapsed:           elapsed.Round(time.Millisecond).String(),
		TotalRequests:     totalRequests,
		SuccessRequests:   successRequests,
		FailedRequests:    failedRequests,
		TotalRecords:      totalRecords,
		TotalBytes:        totalBytes,
		RequestsPerSecond: fmt.Sprintf("%.2f", float64(totalRequests)/elapsedSeconds),
		RecordsPerSecond:  fmt.Sprintf("%.2f", float64(totalRecords)/elapsedSeconds),
		MiBPerSecond:      fmt.Sprintf("%.2f", float64(totalBytes)/(1024*1024)/elapsedSeconds),
		FailureRate:       fmt.Sprintf("%.2f%%", failureRate),
		LatencyP50:        p50.Round(time.Millisecond).String(),
		LatencyP95:        p95.Round(time.Millisecond).String(),
		LatencyP99:        p99.Round(time.Millisecond).String(),
		LatencySamples:    len(latencies),
		LatencyP50Millis:  fmt.Sprintf("%.2f", float64(p50)/float64(time.Millisecond)),
		LatencyP95Millis:  fmt.Sprintf("%.2f", float64(p95)/float64(time.Millisecond)),
		LatencyP99Millis:  fmt.Sprintf("%.2f", float64(p99)/float64(time.Millisecond)),
	}
}

func printSummary(summary benchSummary) {
	fmt.Printf("elapsed=%s\n", summary.Elapsed)
	fmt.Printf("requests=%d success=%d failed=%d\n", summary.TotalRequests, summary.SuccessRequests, summary.FailedRequests)
	fmt.Printf("records=%d request_bytes=%d\n", summary.TotalRecords, summary.TotalBytes)
	fmt.Printf("requests_per_sec=%s\n", summary.RequestsPerSecond)
	fmt.Printf("records_per_sec=%s\n", summary.RecordsPerSecond)
	fmt.Printf("request_mib_per_sec=%s\n", summary.MiBPerSecond)
	if summary.LatencySamples > 0 {
		fmt.Printf("latency_p50=%s latency_p95=%s latency_p99=%s samples=%d\n",
			summary.LatencyP50,
			summary.LatencyP95,
			summary.LatencyP99,
			summary.LatencySamples,
		)
	}
}

func writeHTMLReport(path string, summary benchSummary) error {
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
	return htmlReportTemplate.Execute(f, summary)
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
      width: min(1120px, calc(100vw - 32px));
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
      font-size: 1.55rem;
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
    }
    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 600; width: 34%; }
    tr:last-child th, tr:last-child td { border-bottom: 0; }
    .bars { display: grid; gap: 12px; }
    .bar-row {
      display: grid;
      grid-template-columns: 82px 1fr 90px;
      gap: 12px;
      align-items: center;
    }
    .track {
      height: 14px;
      border-radius: 999px;
      background: color-mix(in srgb, var(--accent) 15%, Canvas);
      overflow: hidden;
      border: 1px solid var(--border);
    }
    .fill {
      height: 100%;
      width: min(100%, calc(var(--v) * 1%));
      background: var(--accent);
      border-radius: inherit;
    }
    code {
      background: color-mix(in srgb, CanvasText 8%, Canvas);
      padding: 2px 5px;
      border-radius: 5px;
    }
    @media (max-width: 760px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .bar-row { grid-template-columns: 70px 1fr; }
      .bar-row strong { grid-column: 2; }
    }
    @media (max-width: 460px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
<main>
  <h1>Kafka REST Proxy Benchmark Report</h1>
  <div class="muted">Generated {{ .GeneratedAt }} for <code>{{ .Config.URL }}/topics/{{ .Config.Topic }}</code></div>

  <section class="grid" aria-label="Benchmark summary">
    <div class="card"><div class="label">Records/sec</div><div class="value">{{ .RecordsPerSecond }}</div></div>
    <div class="card"><div class="label">Requests/sec</div><div class="value">{{ .RequestsPerSecond }}</div></div>
    <div class="card"><div class="label">p99 latency</div><div class="value">{{ .LatencyP99 }}</div></div>
    <div class="card"><div class="label">Failure rate</div><div class="value {{ if eq .FailedRequests 0 }}ok{{ else }}bad{{ end }}">{{ .FailureRate }}</div></div>
  </section>

  <h2>Latency</h2>
  <section class="card bars" aria-label="Latency percentiles">
    <div class="bar-row"><span>p50</span><div class="track"><div class="fill" style="--v: {{ .LatencyP50Millis }}"></div></div><strong>{{ .LatencyP50 }}</strong></div>
    <div class="bar-row"><span>p95</span><div class="track"><div class="fill" style="--v: {{ .LatencyP95Millis }}"></div></div><strong>{{ .LatencyP95 }}</strong></div>
    <div class="bar-row"><span>p99</span><div class="track"><div class="fill" style="--v: {{ .LatencyP99Millis }}"></div></div><strong>{{ .LatencyP99 }}</strong></div>
    <div class="muted">Bar scale uses milliseconds as percentage width and caps visually at 100ms.</div>
  </section>

  <h2>Run details</h2>
  <table>
    <tr><th>Elapsed</th><td>{{ .Elapsed }}</td></tr>
    <tr><th>Requests</th><td>{{ .TotalRequests }} total, {{ .SuccessRequests }} success, {{ .FailedRequests }} failed</td></tr>
    <tr><th>Records</th><td>{{ .TotalRecords }}</td></tr>
    <tr><th>Request bytes</th><td>{{ .TotalBytes }}</td></tr>
    <tr><th>Request MiB/sec</th><td>{{ .MiBPerSecond }}</td></tr>
    <tr><th>Clients</th><td>{{ .Config.Clients }}</td></tr>
    <tr><th>Records/request</th><td>{{ .Config.Records }}</td></tr>
    <tr><th>Payload bytes/record</th><td>{{ .Config.PayloadBytes }}</td></tr>
    <tr><th>Configured duration</th><td>{{ .Config.Duration }}</td></tr>
    <tr><th>Request timeout</th><td>{{ .Config.Timeout }}</td></tr>
    <tr><th>Latency samples</th><td>{{ .LatencySamples }}</td></tr>
  </table>
</main>
</body>
</html>
`))
