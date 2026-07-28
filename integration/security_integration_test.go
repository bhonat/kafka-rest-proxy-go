package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSecurityIntegrationSASLProduceConsume(t *testing.T) {
	if os.Getenv("KAFKA_SECURITY_INTEGRATION") != "1" {
		t.Skip("set KAFKA_SECURITY_INTEGRATION=1 to run SASL security integration tests")
	}

	baseURL := envString("REST_PROXY_SECURITY_URL", "http://localhost:18080")
	projectDir := envString("COMPOSE_SECURITY_PROJECT_DIR", "..")
	waitReady(t, baseURL, 90*time.Second)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-secure-" + runID
	createSecureIntegrationTopic(t, projectDir, topic, 3)

	body := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{"partition": 0, "value": map[string]any{"secure": true, "id": runID}},
		},
	})
	produceResp := produceMatrixRecords(t, baseURL, topic, "application/vnd.kafka.json.v2+json", body)
	if len(produceResp.Offsets) != 1 || produceResp.Offsets[0].ErrorCode != nil || produceResp.Offsets[0].Offset == nil {
		t.Fatalf("unexpected secure produce response: %+v", produceResp)
	}

	consumed := consumeSecureIntegrationRecords(t, projectDir, topic, 1)
	if !strings.Contains(consumed, runID) || !strings.Contains(consumed, `"secure":true`) {
		t.Fatalf("secure consumed output missing expected record:\n%s", consumed)
	}
}

func TestSecurityIntegrationBadCredentials(t *testing.T) {
	if os.Getenv("KAFKA_SECURITY_INTEGRATION") != "1" || os.Getenv("KAFKA_SECURITY_BAD_CREDENTIALS") != "1" {
		t.Skip("set KAFKA_SECURITY_INTEGRATION=1 and KAFKA_SECURITY_BAD_CREDENTIALS=1 to run bad credential diagnostics")
	}

	badURL := envString("REST_PROXY_SECURITY_BAD_URL", "http://localhost:18081")
	waitStatusNotOK(t, badURL+"/readyz", 45*time.Second)
}

func createSecureIntegrationTopic(t *testing.T, projectDir, topic string, partitions int) {
	t.Helper()
	runDockerComposeSecurity(t, projectDir,
		"exec", "-T", "kafka-secure",
		"bash", "-lc",
		securityClientPropertiesCommand()+
			" kafka-topics --bootstrap-server localhost:29094 --command-config /tmp/security-client.properties --create --if-not-exists --topic "+shellQuote(topic)+" --partitions "+strconv.Itoa(partitions)+" --replication-factor 1",
	)
}

func consumeSecureIntegrationRecords(t *testing.T, projectDir, topic string, records int) string {
	t.Helper()
	return runDockerComposeSecurity(t, projectDir,
		"exec", "-T", "kafka-secure",
		"bash", "-lc",
		securityClientPropertiesCommand()+
			" kafka-console-consumer --bootstrap-server localhost:29094 --consumer.config /tmp/security-client.properties --topic "+shellQuote(topic)+" --from-beginning --max-messages "+strconv.Itoa(records)+" --timeout-ms 15000",
	)
}

func runDockerComposeSecurity(t *testing.T, projectDir string, args ...string) string {
	t.Helper()
	cmdArgs := append([]string{"compose", "-f", "docker-compose.security.yml"}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = filepath.Clean(projectDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s failed: %v\n%s", strings.Join(cmdArgs, " "), err, string(out))
	}
	return string(out)
}

func securityClientPropertiesCommand() string {
	return "cat >/tmp/security-client.properties <<'EOF'\n" +
		"security.protocol=SASL_PLAINTEXT\n" +
		"sasl.mechanism=PLAIN\n" +
		"sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\n" +
		"EOF\n"
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
