package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestComposeProduceAndConsume(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("set KAFKA_INTEGRATION=1 to run Docker Compose integration tests")
	}

	baseURL := envString("REST_PROXY_URL", "http://localhost:8080")
	projectDir := envString("COMPOSE_PROJECT_DIR", "..")
	topic := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	payloadID := fmt.Sprintf("compose-integration-%d", time.Now().UnixNano())

	runDockerCompose(t, projectDir,
		"exec", "-T", "kafka",
		"kafka-topics",
		"--bootstrap-server", "localhost:29092",
		"--create",
		"--if-not-exists",
		"--topic", topic,
		"--partitions", "3",
		"--replication-factor", "1",
	)

	waitReady(t, baseURL, 60*time.Second)

	body := map[string]any{
		"records": []map[string]any{
			{
				"key": payloadID,
				"value": map[string]any{
					"id": payloadID,
					"ok": true,
				},
			},
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/topics/"+topic, bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("produce status = %d body=%s", resp.StatusCode, string(respBody))
	}
	var produceResp struct {
		Offsets []struct {
			Partition int32   `json:"partition"`
			Offset    int64   `json:"offset"`
			ErrorCode *int16  `json:"error_code"`
			Error     *string `json:"error"`
		} `json:"offsets"`
	}
	if err := json.Unmarshal(respBody, &produceResp); err != nil {
		t.Fatal(err)
	}
	if len(produceResp.Offsets) != 1 {
		t.Fatalf("offset count = %d, want 1; body=%s", len(produceResp.Offsets), string(respBody))
	}
	if produceResp.Offsets[0].ErrorCode != nil || produceResp.Offsets[0].Error != nil {
		t.Fatalf("produce returned record error: %+v", produceResp.Offsets[0])
	}

	consumed := runDockerCompose(t, projectDir,
		"exec", "-T", "kafka",
		"kafka-console-consumer",
		"--bootstrap-server", "localhost:29092",
		"--topic", topic,
		"--from-beginning",
		"--max-messages", "1",
		"--timeout-ms", "10000",
	)
	if !strings.Contains(consumed, payloadID) {
		t.Fatalf("consumed message does not contain payload id %q; output=%s", payloadID, consumed)
	}
}

func waitReady(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/readyz", nil)
		if err == nil {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for %s/readyz", baseURL)
}

func runDockerCompose(t *testing.T, projectDir string, args ...string) string {
	t.Helper()
	cmdArgs := append([]string{"compose"}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = filepath.Clean(projectDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s failed: %v\n%s", strings.Join(cmdArgs, " "), err, string(out))
	}
	return string(out)
}

func envString(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}
