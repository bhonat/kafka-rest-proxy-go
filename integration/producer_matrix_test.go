package integration_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type matrixProduceResponse struct {
	Offsets []struct {
		Partition *int32  `json:"partition"`
		Offset    *int64  `json:"offset"`
		ErrorCode *int    `json:"error_code"`
		Error     *string `json:"error"`
	} `json:"offsets"`
}

func TestComposeProducerMatrix(t *testing.T) {
	if !integrationEnabled() {
		t.Skip("set KAFKA_INTEGRATION=1 to run Docker Compose integration tests")
	}

	baseURL := envString("REST_PROXY_URL", "http://localhost:8080")
	projectDir := envString("COMPOSE_PROJECT_DIR", "..")
	waitReady(t, baseURL, 60*time.Second)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	jsonID := "json-matrix-" + runID
	binaryValueOne := "bin-value-" + runID + "-one"
	binaryValueTwo := "bin-value-" + runID + "-two"

	jsonBody := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{
				"partition": 0,
				"key":       "json-key-" + runID,
				"value": map[string]any{
					"id":   jsonID,
					"kind": "json",
					"n":    1,
				},
			},
			{
				"partition": 2,
				"value": map[string]any{
					"id":   jsonID,
					"kind": "json",
					"n":    2,
				},
			},
		},
	})
	binaryBody := mustJSON(t, map[string]any{
		"records": []map[string]any{
			{
				"partition": 1,
				"key":       base64.StdEncoding.EncodeToString([]byte("bin-key-" + runID)),
				"value":     base64.StdEncoding.EncodeToString([]byte(binaryValueOne)),
			},
			{
				"partition": 2,
				"value":     base64.StdEncoding.EncodeToString([]byte(binaryValueTwo)),
			},
		},
	})

	tests := []struct {
		name           string
		topic          string
		contentType    string
		body           []byte
		wantPartitions []int32
		wantValues     []string
	}{
		{
			name:           "json",
			topic:          "integration-json-" + runID,
			contentType:    "application/vnd.kafka.json.v2+json",
			body:           jsonBody,
			wantPartitions: []int32{0, 2},
			wantValues:     []string{jsonID, `"kind":"json"`, `"n":1`, `"n":2`},
		},
		{
			name:           "binary",
			topic:          "integration-binary-" + runID,
			contentType:    "application/vnd.kafka.binary.v2+json",
			body:           binaryBody,
			wantPartitions: []int32{1, 2},
			wantValues:     []string{binaryValueOne, binaryValueTwo},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			createIntegrationTopic(t, projectDir, tc.topic, 3)

			produceResp := produceMatrixRecords(t, baseURL, tc.topic, tc.contentType, tc.body)
			if len(produceResp.Offsets) != len(tc.wantPartitions) {
				t.Fatalf("offset count = %d, want %d", len(produceResp.Offsets), len(tc.wantPartitions))
			}
			for i, wantPartition := range tc.wantPartitions {
				got := produceResp.Offsets[i]
				if got.ErrorCode != nil || got.Error != nil {
					t.Fatalf("record %d returned Kafka error: %+v", i, got)
				}
				if got.Partition == nil || *got.Partition != wantPartition {
					t.Fatalf("record %d partition = %v, want %d", i, got.Partition, wantPartition)
				}
				if got.Offset == nil {
					t.Fatalf("record %d offset is nil", i)
				}
			}

			consumed := consumeIntegrationRecords(t, projectDir, tc.topic, len(tc.wantPartitions))
			for _, want := range tc.wantValues {
				if !strings.Contains(consumed, want) {
					t.Fatalf("consumed output missing %q:\n%s", want, consumed)
				}
			}
			processedLine := fmt.Sprintf("Processed a total of %d messages", len(tc.wantPartitions))
			if !strings.Contains(consumed, processedLine) {
				t.Fatalf("consumer did not report expected record count %q:\n%s", processedLine, consumed)
			}
		})
	}
}

func integrationEnabled() bool {
	return strings.TrimSpace(envString("KAFKA_INTEGRATION", "")) == "1"
}

func createIntegrationTopic(t *testing.T, projectDir, topic string, partitions int) {
	t.Helper()
	runDockerCompose(t, projectDir,
		"exec", "-T", "kafka",
		"kafka-topics",
		"--bootstrap-server", "localhost:29092",
		"--create",
		"--if-not-exists",
		"--topic", topic,
		"--partitions", strconv.Itoa(partitions),
		"--replication-factor", "1",
	)
}

func produceMatrixRecords(t *testing.T, baseURL, topic, contentType string, body []byte) matrixProduceResponse {
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("produce status = %d body=%s", resp.StatusCode, string(respBody))
	}

	var produceResp matrixProduceResponse
	if err := json.Unmarshal(respBody, &produceResp); err != nil {
		t.Fatalf("decode produce response: %v body=%s", err, string(respBody))
	}
	return produceResp
}

func consumeIntegrationRecords(t *testing.T, projectDir, topic string, records int) string {
	t.Helper()
	return runDockerCompose(t, projectDir,
		"exec", "-T", "kafka",
		"kafka-console-consumer",
		"--bootstrap-server", "localhost:29092",
		"--topic", topic,
		"--from-beginning",
		"--max-messages", strconv.Itoa(records),
		"--timeout-ms", "15000",
		"--property", "print.key=true",
		"--property", "key.separator=|",
	)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
