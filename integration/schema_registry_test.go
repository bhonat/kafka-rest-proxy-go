package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestComposeSchemaRegistryAvroProduce(t *testing.T) {
	env := schemaRegistryIntegrationEnv(t)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-schema-" + runID
	marker := "schema-live-" + runID
	createIntegrationTopic(t, env.projectDir, topic, 3)

	schemaText := `{"type":"record","name":"SchemaLive","fields":[{"name":"id","type":"string"},{"name":"ok","type":"boolean"}]}`
	body := mustJSON(t, map[string]any{
		"value_schema": schemaText,
		"records": []map[string]any{
			{
				"partition": 0,
				"value": map[string]any{
					"id": marker,
					"ok": true,
				},
			},
		},
	})

	produceResp := assertSchemaProduce(t, env, topic, "application/vnd.kafka.avro.v2+json", body)

	value := consumeRawRecordValue(t, env.brokers, topic, 0, 20*time.Second)
	assertConfluentWireValue(t, value, *produceResp.ValueSchemaID)
	if !bytes.Contains(value, []byte(marker)) {
		t.Fatalf("raw Avro payload does not contain marker %q; value=%x", marker, value)
	}
}

func TestComposeSchemaRegistryJSONSchemaProduce(t *testing.T) {
	env := schemaRegistryIntegrationEnv(t)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-jsonschema-" + runID
	marker := "jsonschema-live-" + runID
	createIntegrationTopic(t, env.projectDir, topic, 3)

	schemaText := `{"title":"SchemaLiveJSON","type":"object","properties":{"id":{"type":"string"},"ok":{"type":"boolean"}},"required":["id","ok"]}`
	body := mustJSON(t, map[string]any{
		"value_schema": schemaText,
		"records": []map[string]any{
			{
				"partition": 0,
				"value": map[string]any{
					"id": marker,
					"ok": true,
				},
			},
		},
	})

	produceResp := assertSchemaProduce(t, env, topic, "application/vnd.kafka.jsonschema.v2+json", body)
	value := consumeRawRecordValue(t, env.brokers, topic, 0, 20*time.Second)
	assertConfluentWireValue(t, value, *produceResp.ValueSchemaID)
	if !bytes.Contains(value, []byte(marker)) {
		t.Fatalf("raw JSON Schema payload does not contain marker %q; value=%x", marker, value)
	}
}

func TestComposeSchemaRegistryProtobufProduce(t *testing.T) {
	env := schemaRegistryIntegrationEnv(t)

	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "integration-protobuf-" + runID
	marker := "protobuf-live-" + runID
	createIntegrationTopic(t, env.projectDir, topic, 3)

	schemaText := `syntax = "proto3"; message SchemaLiveProto { string id = 1; bool ok = 2; }`
	body := mustJSON(t, map[string]any{
		"value_schema": schemaText,
		"records": []map[string]any{
			{
				"partition": 0,
				"value": map[string]any{
					"id": marker,
					"ok": true,
				},
			},
		},
	})

	produceResp := assertSchemaProduce(t, env, topic, "application/vnd.kafka.protobuf.v2+json", body)
	value := consumeRawRecordValue(t, env.brokers, topic, 0, 20*time.Second)
	assertConfluentWireValue(t, value, *produceResp.ValueSchemaID)
	if !bytes.Contains(value, []byte(marker)) {
		t.Fatalf("raw Protobuf payload does not contain marker %q; value=%x", marker, value)
	}
}

type schemaRegistryIntegration struct {
	baseURL    string
	brokers    string
	projectDir string
}

func schemaRegistryIntegrationEnv(t *testing.T) schemaRegistryIntegration {
	t.Helper()
	if strings.TrimSpace(envString("KAFKA_SCHEMA_REGISTRY_INTEGRATION", "")) != "1" {
		t.Skip("set KAFKA_SCHEMA_REGISTRY_INTEGRATION=1 to run Schema Registry integration tests")
	}

	env := schemaRegistryIntegration{
		baseURL:    envString("REST_PROXY_URL", "http://localhost:8080"),
		brokers:    envString("KAFKA_SCHEMA_REGISTRY_BROKERS", "localhost:9092"),
		projectDir: envString("COMPOSE_PROJECT_DIR", ".."),
	}
	registryURL := envString("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	waitReady(t, env.baseURL, 60*time.Second)
	waitSchemaRegistry(t, registryURL, 60*time.Second)
	return env
}

func assertSchemaProduce(t *testing.T, env schemaRegistryIntegration, topic, contentType string, body []byte) schemaProduceResponse {
	t.Helper()
	produceResp := produceSchemaRecords(t, env.baseURL, topic, contentType, body)
	if len(produceResp.Offsets) != 1 {
		t.Fatalf("offset count = %d, want 1", len(produceResp.Offsets))
	}
	if produceResp.Offsets[0].ErrorCode != nil || produceResp.Offsets[0].Error != nil {
		t.Fatalf("record returned Kafka error: %+v", produceResp.Offsets[0])
	}
	if produceResp.ValueSchemaID == nil || *produceResp.ValueSchemaID <= 0 {
		t.Fatalf("value_schema_id = %#v, want positive id", produceResp.ValueSchemaID)
	}
	return produceResp
}

type schemaProduceResponse struct {
	Offsets []struct {
		Partition *int32  `json:"partition"`
		Offset    *int64  `json:"offset"`
		ErrorCode *int    `json:"error_code"`
		Error     *string `json:"error"`
	} `json:"offsets"`
	KeySchemaID   *int `json:"key_schema_id"`
	ValueSchemaID *int `json:"value_schema_id"`
}

func produceSchemaRecords(t *testing.T, baseURL, topic, contentType string, body []byte) schemaProduceResponse {
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
		t.Fatalf("schema produce status = %d body=%s", resp.StatusCode, string(respBody))
	}

	var produceResp schemaProduceResponse
	if err := json.Unmarshal(respBody, &produceResp); err != nil {
		t.Fatalf("decode schema produce response: %v body=%s", err, string(respBody))
	}
	return produceResp
}

func waitSchemaRegistry(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	url := strings.TrimRight(baseURL, "/") + "/subjects"
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
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
	t.Fatalf("timed out waiting for Schema Registry at %s", url)
}

func consumeRawRecordValue(t *testing.T, brokers, topic string, partition int32, timeout time.Duration) []byte {
	t.Helper()
	seedBrokers := strings.Split(brokers, ",")
	for i := range seedBrokers {
		seedBrokers[i] = strings.TrimSpace(seedBrokers[i])
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.ClientID("schema-registry-integration-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {
				partition: kgo.NewOffset().AtStart(),
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				break
			}
			t.Fatalf("poll raw record: %v", err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			if record.Topic == topic && record.Partition == partition {
				return append([]byte(nil), record.Value...)
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	t.Fatalf("timed out waiting for raw record on %s/%d", topic, partition)
	return nil
}

func assertConfluentWireValue(t *testing.T, value []byte, wantSchemaID int) {
	t.Helper()
	if len(value) < 6 {
		t.Fatalf("value too short for Confluent wire format: %x", value)
	}
	if value[0] != 0 {
		t.Fatalf("wire magic byte = %d, want 0; value=%x", value[0], value)
	}
	gotSchemaID := int(binary.BigEndian.Uint32(value[1:5]))
	if gotSchemaID != wantSchemaID {
		t.Fatalf("wire schema id = %d, want %d; value=%x", gotSchemaID, wantSchemaID, value)
	}
	if len(value[5:]) == 0 {
		t.Fatalf("wire payload is empty after schema header")
	}
}
