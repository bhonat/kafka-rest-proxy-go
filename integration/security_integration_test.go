package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
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

func TestSecurityIntegrationSASLSSLProduceConsume(t *testing.T) {
	if os.Getenv("KAFKA_SECURITY_INTEGRATION") != "1" || os.Getenv("KAFKA_SASL_SSL_INTEGRATION") != "1" {
		t.Skip("set KAFKA_SECURITY_INTEGRATION=1 and KAFKA_SASL_SSL_INTEGRATION=1 to run SASL_SSL integration tests")
	}

	baseURL := envString("REST_PROXY_SASL_SSL_URL", "http://localhost:18082")
	projectDir := envString("COMPOSE_SECURITY_PROJECT_DIR", "..")
	waitReady(t, baseURL, 90*time.Second)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-sasl-ssl-" + runID
	createSASLSSLIntegrationTopic(t, projectDir, topic, 3)

	body := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{"partition": 0, "value": map[string]any{"sasl_ssl": true, "id": runID}},
		},
	})
	produceResp := produceMatrixRecords(t, baseURL, topic, "application/vnd.kafka.json.v2+json", body)
	if len(produceResp.Offsets) != 1 || produceResp.Offsets[0].ErrorCode != nil || produceResp.Offsets[0].Offset == nil {
		t.Fatalf("unexpected SASL_SSL produce response: %+v", produceResp)
	}

	consumed := consumeSASLSSLIntegrationRecords(t, projectDir, topic, 1)
	if !strings.Contains(consumed, runID) || !strings.Contains(consumed, `"sasl_ssl":true`) {
		t.Fatalf("SASL_SSL consumed output missing expected record:\n%s", consumed)
	}
}

func TestSecurityIntegrationMTLSProduceConsume(t *testing.T) {
	if os.Getenv("KAFKA_SECURITY_INTEGRATION") != "1" || os.Getenv("KAFKA_MTLS_INTEGRATION") != "1" {
		t.Skip("set KAFKA_SECURITY_INTEGRATION=1 and KAFKA_MTLS_INTEGRATION=1 to run mTLS integration tests")
	}

	baseURL := envString("REST_PROXY_MTLS_URL", "http://localhost:18084")
	projectDir := envString("COMPOSE_SECURITY_PROJECT_DIR", "..")
	waitReady(t, baseURL, 90*time.Second)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-mtls-" + runID
	createMTLSIntegrationTopic(t, projectDir, topic, 3)

	body := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{"partition": 0, "value": map[string]any{"mtls": true, "id": runID}},
		},
	})
	produceResp := produceMatrixRecords(t, baseURL, topic, "application/vnd.kafka.json.v2+json", body)
	if len(produceResp.Offsets) != 1 || produceResp.Offsets[0].ErrorCode != nil || produceResp.Offsets[0].Offset == nil {
		t.Fatalf("unexpected mTLS produce response: %+v", produceResp)
	}

	consumed := consumeMTLSIntegrationRecords(t, projectDir, topic, 1)
	if !strings.Contains(consumed, runID) || !strings.Contains(consumed, `"mtls":true`) {
		t.Fatalf("mTLS consumed output missing expected record:\n%s", consumed)
	}
}

func TestSecurityIntegrationACLAllowDeny(t *testing.T) {
	if os.Getenv("KAFKA_SECURITY_INTEGRATION") != "1" || os.Getenv("KAFKA_ACL_INTEGRATION") != "1" {
		t.Skip("set KAFKA_SECURITY_INTEGRATION=1 and KAFKA_ACL_INTEGRATION=1 to run ACL integration tests")
	}

	baseURL := envString("REST_PROXY_ACL_URL", "http://localhost:18083")
	waitReady(t, baseURL, 90*time.Second)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	allowedBody := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{"value": map[string]any{"acl": "allowed", "id": runID}},
		},
	})
	allowedStatus, allowedResp, allowedRaw := produceSecurityRecords(t, baseURL, "acl-allowed", "application/vnd.kafka.json.v2+json", allowedBody)
	if allowedStatus != http.StatusOK {
		t.Fatalf("allowed produce status = %d body=%s", allowedStatus, allowedRaw)
	}
	if len(allowedResp.Offsets) != 1 || allowedResp.Offsets[0].ErrorCode != nil || allowedResp.Offsets[0].Offset == nil {
		t.Fatalf("unexpected ACL allowed produce response: %+v body=%s", allowedResp, allowedRaw)
	}

	deniedBody := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{"value": map[string]any{"acl": "denied", "id": runID}},
		},
	})
	deniedStatus, deniedResp, deniedRaw := produceSecurityRecords(t, baseURL, "acl-denied", "application/vnd.kafka.json.v2+json", deniedBody)
	if deniedStatus != http.StatusOK {
		t.Fatalf("denied Kafka record failure should use Confluent-style HTTP 200; status=%d body=%s", deniedStatus, deniedRaw)
	}
	if len(deniedResp.Offsets) != 1 {
		t.Fatalf("denied offsets = %d, want 1; body=%s", len(deniedResp.Offsets), deniedRaw)
	}
	if deniedResp.Offsets[0].ErrorCode == nil || deniedResp.Offsets[0].Error == nil {
		t.Fatalf("denied produce should return per-record error_code/error; response=%+v body=%s", deniedResp, deniedRaw)
	}
	if deniedResp.Offsets[0].Offset != nil {
		t.Fatalf("denied produce should not return an offset; response=%+v body=%s", deniedResp, deniedRaw)
	}
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

func createSASLSSLIntegrationTopic(t *testing.T, projectDir, topic string, partitions int) {
	t.Helper()
	runDockerComposeSecurity(t, projectDir,
		"exec", "-T", "kafka-sasl-ssl",
		"bash", "-lc",
		saslSSLClientPropertiesCommand()+
			" kafka-topics --bootstrap-server localhost:29095 --command-config /tmp/security-client.properties --create --if-not-exists --topic "+shellQuote(topic)+" --partitions "+strconv.Itoa(partitions)+" --replication-factor 1",
	)
}

func consumeSASLSSLIntegrationRecords(t *testing.T, projectDir, topic string, records int) string {
	t.Helper()
	return runDockerComposeSecurity(t, projectDir,
		"exec", "-T", "kafka-sasl-ssl",
		"bash", "-lc",
		saslSSLClientPropertiesCommand()+
			" kafka-console-consumer --bootstrap-server localhost:29095 --consumer.config /tmp/security-client.properties --topic "+shellQuote(topic)+" --from-beginning --max-messages "+strconv.Itoa(records)+" --timeout-ms 15000",
	)
}

func createMTLSIntegrationTopic(t *testing.T, projectDir, topic string, partitions int) {
	t.Helper()
	runDockerComposeSecurity(t, projectDir,
		"exec", "-T", "kafka-mtls",
		"bash", "-lc",
		mtlsClientPropertiesCommand()+
			" kafka-topics --bootstrap-server localhost:29097 --command-config /tmp/mtls-client.properties --create --if-not-exists --topic "+shellQuote(topic)+" --partitions "+strconv.Itoa(partitions)+" --replication-factor 1",
	)
}

func consumeMTLSIntegrationRecords(t *testing.T, projectDir, topic string, records int) string {
	t.Helper()
	return runDockerComposeSecurity(t, projectDir,
		"exec", "-T", "kafka-mtls",
		"bash", "-lc",
		mtlsClientPropertiesCommand()+
			" kafka-console-consumer --bootstrap-server localhost:29097 --consumer.config /tmp/mtls-client.properties --topic "+shellQuote(topic)+" --from-beginning --max-messages "+strconv.Itoa(records)+" --timeout-ms 15000",
	)
}

func produceSecurityRecords(t *testing.T, baseURL, topic, contentType string, body []byte) (int, matrixProduceResponse, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/topics/"+topic, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var produceResp matrixProduceResponse
	if strings.Contains(resp.Header.Get("Content-Type"), "json") {
		_ = json.Unmarshal(respBody, &produceResp)
	}
	return resp.StatusCode, produceResp, string(respBody)
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

func saslSSLClientPropertiesCommand() string {
	return "cat >/tmp/security-client.properties <<'EOF'\n" +
		"security.protocol=SASL_SSL\n" +
		"sasl.mechanism=PLAIN\n" +
		"sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\n" +
		"ssl.truststore.location=/etc/kafka/secrets/broker.truststore.p12\n" +
		"ssl.truststore.password=test-secret\n" +
		"ssl.truststore.type=PKCS12\n" +
		"EOF\n"
}

func mtlsClientPropertiesCommand() string {
	return "cat >/tmp/mtls-client.properties <<'EOF'\n" +
		"security.protocol=SSL\n" +
		"ssl.truststore.location=/etc/kafka/secrets/broker.truststore.p12\n" +
		"ssl.truststore.password=test-secret\n" +
		"ssl.truststore.type=PKCS12\n" +
		"ssl.keystore.location=/etc/kafka/secrets/client.keystore.p12\n" +
		"ssl.keystore.password=test-secret\n" +
		"ssl.keystore.type=PKCS12\n" +
		"ssl.key.password=test-secret\n" +
		"EOF\n"
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
