package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestComposeProducerLoad(t *testing.T) {
	if strings.TrimSpace(os.Getenv("KAFKA_LOAD_INTEGRATION")) != "1" {
		t.Skip("set KAFKA_LOAD_INTEGRATION=1 to run the Compose producer load test")
	}
	if !integrationEnabled() {
		t.Skip("set KAFKA_INTEGRATION=1 to run Docker Compose integration tests")
	}

	baseURL := envString("REST_PROXY_URL", "http://localhost:8080")
	projectDir := envString("COMPOSE_PROJECT_DIR", "..")
	clients := envInt("KAFKA_LOAD_CLIENTS", 8)
	recordsPerRequest := envInt("KAFKA_LOAD_RECORDS_PER_REQUEST", 10)
	duration := envDuration("KAFKA_LOAD_DURATION", 15*time.Second)
	minRecordsPerSecond := envFloat("KAFKA_LOAD_MIN_RECORDS_PER_SECOND", 1000)
	maxFailureRate := envFloat("KAFKA_LOAD_MAX_FAILURE_RATE", 0)

	waitReady(t, baseURL, 60*time.Second)
	topic := fmt.Sprintf("integration-load-%d", time.Now().UnixNano())
	createIntegrationTopic(t, projectDir, topic, 12)

	body := loadBody(t, recordsPerRequest)
	client := &http.Client{Timeout: 30 * time.Second}
	deadline := time.Now().Add(duration)

	var attemptedRecords atomic.Int64
	var successfulRecords atomic.Int64
	var failedRecords atomic.Int64
	var requests atomic.Int64

	var wg sync.WaitGroup
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				requests.Add(1)
				attemptedRecords.Add(int64(recordsPerRequest))
				success, failed := postLoadRequest(t, client, baseURL, topic, body)
				successfulRecords.Add(success)
				failedRecords.Add(failed)
			}
		}()
	}
	wg.Wait()

	success := successfulRecords.Load()
	failed := failedRecords.Load()
	attempted := attemptedRecords.Load()
	if attempted == 0 {
		t.Fatal("no records attempted")
	}
	elapsedRecordsPerSecond := float64(success) / duration.Seconds()
	failureRate := float64(failed) / float64(attempted)

	t.Logf("load summary requests=%d attempted_records=%d successful_records=%d failed_records=%d records_per_sec=%.2f failure_rate=%.6f",
		requests.Load(), attempted, success, failed, elapsedRecordsPerSecond, failureRate)

	if failureRate > maxFailureRate {
		t.Fatalf("failure rate %.6f exceeds max %.6f", failureRate, maxFailureRate)
	}
	if minRecordsPerSecond > 0 && elapsedRecordsPerSecond < minRecordsPerSecond {
		t.Fatalf("records/sec %.2f below minimum %.2f", elapsedRecordsPerSecond, minRecordsPerSecond)
	}
}

func loadBody(t *testing.T, records int) []byte {
	t.Helper()
	items := make([]map[string]any, 0, records)
	for i := 0; i < records; i++ {
		items = append(items, map[string]any{
			"key": fmt.Sprintf("load-key-%d", i),
			"value": map[string]any{
				"i":  i,
				"ok": true,
			},
		})
	}
	body, err := json.Marshal(map[string]any{"records": items})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func postLoadRequest(t *testing.T, client *http.Client, baseURL, topic string, body []byte) (success, failed int64) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/topics/"+topic, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, int64(countLoadRecords(body))
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, int64(countLoadRecords(body))
	}
	var decoded matrixProduceResponse
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return 0, int64(countLoadRecords(body))
	}
	for _, offset := range decoded.Offsets {
		if offset.ErrorCode != nil || offset.Error != nil {
			failed++
		} else {
			success++
		}
	}
	return success, failed
}

func countLoadRecords(body []byte) int {
	var decoded struct {
		Records []json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return 1
	}
	return len(decoded.Records)
}

func envInt(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envFloat(name string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(name string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
