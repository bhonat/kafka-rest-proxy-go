package integration_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestComposeKafkaFailureRecovery(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" || os.Getenv("KAFKA_FAILURE_INTEGRATION") != "1" {
		t.Skip("set KAFKA_INTEGRATION=1 and KAFKA_FAILURE_INTEGRATION=1 to run Kafka failure integration tests")
	}

	baseURL := envString("REST_PROXY_URL", "http://localhost:8080")
	projectDir := envString("COMPOSE_PROJECT_DIR", "..")

	waitReady(t, baseURL, 60*time.Second)
	t.Cleanup(func() {
		runDockerCompose(t, projectDir, "start", "kafka")
		waitReady(t, baseURL, 90*time.Second)
	})

	runDockerCompose(t, projectDir, "stop", "kafka")
	waitStatusNotOK(t, baseURL+"/readyz", 45*time.Second)

	runDockerCompose(t, projectDir, "start", "kafka")
	waitReady(t, baseURL, 90*time.Second)

	topic := "integration-failure-" + strings.ReplaceAll(time.Now().Format("20060102150405.000000000"), ".", "")
	createIntegrationTopic(t, projectDir, topic, 3)
	body := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{"value": map[string]any{"recovered": true, "topic": topic}},
		},
	})
	resp := produceMatrixRecords(t, baseURL, topic, "application/vnd.kafka.json.v2+json", body)
	if len(resp.Offsets) != 1 || resp.Offsets[0].ErrorCode != nil || resp.Offsets[0].Offset == nil {
		t.Fatalf("unexpected recovery produce response: %+v", resp)
	}
	consumed := consumeIntegrationRecords(t, projectDir, topic, 1)
	if !strings.Contains(consumed, `"recovered":true`) {
		t.Fatalf("recovery produce was not consumed:\n%s", consumed)
	}
}

func waitStatusNotOK(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				cancel()
				if resp.StatusCode != http.StatusOK {
					return
				}
			} else {
				cancel()
				return
			}
		} else {
			cancel()
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s stayed healthy for %s after Kafka was stopped", url, timeout)
}
