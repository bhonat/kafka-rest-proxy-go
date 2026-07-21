package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
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

	fmt.Printf("elapsed=%s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("requests=%d success=%d failed=%d\n", totalRequests, successRequests, failedRequests)
	fmt.Printf("records=%d request_bytes=%d\n", totalRecords, totalBytes)
	fmt.Printf("requests_per_sec=%.2f\n", float64(totalRequests)/elapsed.Seconds())
	fmt.Printf("records_per_sec=%.2f\n", float64(totalRecords)/elapsed.Seconds())
	fmt.Printf("request_mib_per_sec=%.2f\n", float64(totalBytes)/(1024*1024)/elapsed.Seconds())
	if len(latencies) > 0 {
		fmt.Printf("latency_p50=%s latency_p95=%s latency_p99=%s samples=%d\n",
			percentile(latencies, 0.50).Round(time.Millisecond),
			percentile(latencies, 0.95).Round(time.Millisecond),
			percentile(latencies, 0.99).Round(time.Millisecond),
			len(latencies),
		)
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
