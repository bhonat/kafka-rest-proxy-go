package compatibility_test

import (
	"os"
	"strings"
	"testing"
)

func TestOpenAPIProducerContractDocumentsSupportedSurface(t *testing.T) {
	b, err := os.ReadFile("../api/v2/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	spec := string(b)

	required := []string{
		"openapi: 3.1.0",
		"/topics/{topic}:",
		"operationId: produceToTopic",
		"/topics/{topic}/partitions/{partition}:",
		"operationId: produceToPartition",
		"application/vnd.kafka.json.v2+json",
		"application/vnd.kafka.binary.v2+json",
		"application/vnd.kafka.avro.v2+json",
		"application/vnd.kafka.protobuf.v2+json",
		"application/vnd.kafka.jsonschema.v2+json",
		"application/vnd.kafka.v2+json",
		"ProduceResponse",
		"offsets",
		"error_code",
		"key_schema_id",
		"value_schema_id",
		"/healthz:",
		"/readyz:",
	}
	for _, want := range required {
		if !strings.Contains(spec, want) {
			t.Fatalf("OpenAPI spec missing %q", want)
		}
	}

	forbidden := []string{
		"\n  /consumers/",
		"\n  /schemas/",
	}
	for _, needle := range forbidden {
		if strings.Contains(spec, needle) {
			t.Fatalf("OpenAPI spec should not document out-of-scope surface %q", needle)
		}
	}

	v3, err := os.ReadFile("../api/v3/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	v3Spec := string(v3)
	v3Required := []string{
		"/v3/clusters/{cluster_id}/topics/{topic_name}/records:",
		"operationId: produceRecord",
		"/v3/clusters/{cluster_id}/topics/{topic_name}/records:batch:",
		"operationId: produceRecordsBatch",
		"AVRO",
		"JSONSCHEMA",
		"PROTOBUF",
		"successes",
		"failures",
	}
	for _, want := range v3Required {
		if !strings.Contains(v3Spec, want) {
			t.Fatalf("v3 OpenAPI spec missing %q", want)
		}
	}
}
