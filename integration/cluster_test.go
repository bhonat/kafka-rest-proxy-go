package integration_test

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestClusterRollingBrokerRestart(t *testing.T) {
	if strings.TrimSpace(envString("KAFKA_CLUSTER_INTEGRATION", "")) != "1" {
		t.Skip("set KAFKA_CLUSTER_INTEGRATION=1 to run 3-broker cluster integration tests")
	}

	baseURL := envString("REST_PROXY_CLUSTER_URL", "http://localhost:8080")
	projectDir := envString("COMPOSE_CLUSTER_PROJECT_DIR", "..")
	waitReady(t, baseURL, 90*time.Second)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-cluster-" + runID
	createClusterIntegrationTopic(t, projectDir, topic, 6, 3)

	produceClusterMarker(t, baseURL, topic, "before-restart-"+runID)

	t.Cleanup(func() {
		_, _ = runDockerComposeClusterNoFatal(projectDir, "start", "kafka-2")
	})
	runDockerComposeCluster(t, projectDir, "stop", "kafka-2")
	waitReady(t, baseURL, 90*time.Second)

	produceClusterMarker(t, baseURL, topic, "during-restart-"+runID)

	runDockerComposeCluster(t, projectDir, "start", "kafka-2")
	waitReady(t, baseURL, 90*time.Second)

	produceClusterMarker(t, baseURL, topic, "after-restart-"+runID)
	consumed := consumeClusterIntegrationRecords(t, projectDir, topic, 3)
	for _, want := range []string{
		"before-restart-" + runID,
		"during-restart-" + runID,
		"after-restart-" + runID,
	} {
		if !strings.Contains(consumed, want) {
			t.Fatalf("cluster consumed output missing %q:\n%s", want, consumed)
		}
	}
}

func createClusterIntegrationTopic(t *testing.T, projectDir, topic string, partitions, replicationFactor int) {
	t.Helper()
	runDockerComposeCluster(t, projectDir,
		"exec", "-T", "kafka-1",
		"kafka-topics",
		"--bootstrap-server", "localhost:9092",
		"--create",
		"--if-not-exists",
		"--topic", topic,
		"--partitions", strconv.Itoa(partitions),
		"--replication-factor", strconv.Itoa(replicationFactor),
	)
}

func produceClusterMarker(t *testing.T, baseURL, topic, marker string) {
	t.Helper()
	body := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{
				"value": map[string]any{
					"id":     marker,
					"marker": marker,
				},
			},
		},
	})
	resp := produceMatrixRecords(t, baseURL, topic, "application/vnd.kafka.json.v2+json", body)
	if len(resp.Offsets) != 1 || resp.Offsets[0].ErrorCode != nil || resp.Offsets[0].Offset == nil {
		t.Fatalf("unexpected cluster produce response: %+v", resp)
	}
}

func consumeClusterIntegrationRecords(t *testing.T, projectDir, topic string, records int) string {
	t.Helper()
	return runDockerComposeCluster(t, projectDir,
		"exec", "-T", "kafka-1",
		"kafka-console-consumer",
		"--bootstrap-server", "localhost:9092",
		"--topic", topic,
		"--from-beginning",
		"--max-messages", strconv.Itoa(records),
		"--timeout-ms", "20000",
	)
}

func runDockerComposeCluster(t *testing.T, projectDir string, args ...string) string {
	t.Helper()
	out, err := runDockerComposeClusterNoFatal(projectDir, args...)
	if err != nil {
		t.Fatalf("docker compose -f docker-compose.cluster.yml %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func runDockerComposeClusterNoFatal(projectDir string, args ...string) (string, error) {
	cmdArgs := append([]string{"compose", "-f", "docker-compose.cluster.yml"}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = filepath.Clean(projectDir)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
