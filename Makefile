VERSION := $(shell cat VERSION)
GO_PATCHED_TOOLCHAIN ?= go1.25.12
export GOCACHE ?= /tmp/kafka-rest-proxy-go-gocache
export GOMODCACHE ?= /tmp/kafka-rest-proxy-go-gomodcache

.PHONY: test fmt-check vet coverage govulncheck staticcheck quality openapi-check licenses-check generate-licenses prepare-security-secrets test-race test-integration test-failure-integration test-cluster-integration test-differential test-differential-full test-schema-registry-integration test-security-integration test-sasl-ssl-integration test-mtls-integration test-acl-integration test-load-integration test-hardening test-soak build build-release generate-sbom sign-release run tidy docker-build compose-up compose-up-cluster compose-up-comparison compose-up-schema-registry compose-up-security compose-up-sasl-ssl compose-up-mtls compose-up-acl compose-down compose-down-cluster compose-down-security compose-smoke bench-produce bench-html bench-suite bench-compare bench-regression capture-confluent-fixtures prepare-benchmark-pages otel-metrics

test:
	go test ./...

fmt-check:
	sh scripts/check-format.sh

vet:
	go vet ./...

coverage:
	go test -coverprofile=coverage.out ./...

govulncheck:
	GOTOOLCHAIN=$(GO_PATCHED_TOOLCHAIN)+auto go run golang.org/x/vuln/cmd/govulncheck@latest ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

quality: fmt-check vet coverage

openapi-check:
	go test ./compatibility -run TestOpenAPIProducerContractDocumentsSupportedSurface -count=1

generate-licenses:
	go run ./cmd/license-bundle -out licenses

licenses-check: generate-licenses
	git diff --exit-code -- licenses
	@test -z "$$(git status --short -- licenses)" || (git status --short -- licenses && exit 1)

prepare-security-secrets:
	testing/secrets/configure.sh

test-race:
	go test -race ./internal/api ./internal/limits ./internal/producer/franz ./compatibility ./cmd/bench-produce ./cmd/soak-produce

test-integration:
	KAFKA_INTEGRATION=1 go test ./integration -v

test-failure-integration:
	KAFKA_INTEGRATION=1 KAFKA_FAILURE_INTEGRATION=1 go test ./integration -run TestComposeKafkaFailureRecovery -count=1 -v

test-cluster-integration:
	KAFKA_CLUSTER_INTEGRATION=1 go test ./integration -run TestClusterRollingBrokerRestart -count=1 -v

test-differential:
	KAFKA_REST_DIFFERENTIAL=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v

test-differential-full:
	KAFKA_REST_DIFFERENTIAL=1 KAFKA_REST_DIFFERENTIAL_V3=1 KAFKA_REST_DIFFERENTIAL_SCHEMA=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v

test-schema-registry-integration:
	KAFKA_SCHEMA_REGISTRY_INTEGRATION=1 go test ./integration -run TestComposeSchemaRegistry -count=1 -v

test-security-integration:
	KAFKA_SECURITY_INTEGRATION=1 go test ./integration -run TestSecurityIntegration -count=1 -v

test-sasl-ssl-integration:
	KAFKA_SECURITY_INTEGRATION=1 KAFKA_SASL_SSL_INTEGRATION=1 go test ./integration -run TestSecurityIntegrationSASLSSLProduceConsume -count=1 -v

test-mtls-integration:
	KAFKA_SECURITY_INTEGRATION=1 KAFKA_MTLS_INTEGRATION=1 go test ./integration -run TestSecurityIntegrationMTLSProduceConsume -count=1 -v

test-acl-integration:
	KAFKA_SECURITY_INTEGRATION=1 KAFKA_ACL_INTEGRATION=1 go test ./integration -run TestSecurityIntegrationACL -count=1 -v

test-load-integration:
	KAFKA_LOAD_INTEGRATION=1 go test ./integration -run TestComposeProducerLoad -count=1 -v

test-hardening: test test-race test-integration test-differential

test-soak:
	go run ./cmd/soak-produce -url http://localhost:8080 -topic orders -duration 2m -warmup 10s -clients 16 -records-per-request 10 -payload-bytes 128 -format json -max-failure-rate 0 -min-records-sec 1000 -max-p99 250ms

build:
	mkdir -p bin
	go build -o bin/kafka-rest-proxy-go ./cmd/kafka-rest-proxy-go
	go build -o bin/bench-produce ./cmd/bench-produce
	go build -o bin/soak-produce ./cmd/soak-produce

build-release:
	sh scripts/build-release.sh

generate-sbom:
	sh scripts/generate-sbom.sh

sign-release:
	sh scripts/sign-release.sh

run:
	go run ./cmd/kafka-rest-proxy-go

tidy:
	go mod tidy

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t kafka-rest-proxy-go:$(VERSION) -t kafka-rest-proxy-go:dev .

compose-up:
	docker compose up --build

compose-up-cluster:
	docker compose -f docker-compose.cluster.yml up --build

compose-up-comparison:
	docker compose --profile comparison up --build

compose-up-schema-registry:
	docker compose --profile schema-registry up --build

compose-up-security:
	docker compose -f docker-compose.security.yml up --build

compose-up-sasl-ssl: prepare-security-secrets
	docker compose -f docker-compose.security.yml --profile sasl-ssl up --build

compose-up-mtls: prepare-security-secrets
	docker compose -f docker-compose.security.yml --profile mtls up --build

compose-up-acl:
	docker compose -f docker-compose.security.yml --profile acl up --build

compose-down:
	docker compose down

compose-down-cluster:
	docker compose -f docker-compose.cluster.yml down

compose-down-security:
	docker compose -f docker-compose.security.yml down

compose-smoke:
	scripts/compose-smoke.sh

bench-produce:
	go run ./cmd/bench-produce -url http://localhost:8080 -topic orders -duration 30s -clients 32 -records 10 -payload-bytes 512

bench-html:
	go run ./cmd/bench-produce -url http://localhost:8080 -topic orders -duration 30s -clients 32 -records 10 -payload-bytes 512 -html dist/benchmark-report.html

bench-suite:
	go run ./cmd/bench-produce -suite -url http://localhost:8080 -topic orders -duration 5s -payload-sizes 128,512,1KiB -records-per-request 1,10,100 -client-counts 4,16 -html dist/benchmark-suite.html

bench-compare:
	go run ./cmd/bench-produce -suite -target go=http://localhost:8080 -target confluent=http://localhost:8082 -topic orders -duration 5s -payload-sizes 128,512,1KiB -records-per-request 1,10,100 -client-counts 4,16 -html dist/benchmark-comparison.html

bench-regression:
	go run ./cmd/bench-produce -suite -target go=http://localhost:8080 -target confluent=http://localhost:8082 -topic orders -duration 30s -payload-sizes 128,512,1KiB,10KiB -records-per-request 1,10,100 -client-counts 4,16,64 -formats json,binary -html dist/benchmark-regression.html

capture-confluent-fixtures:
	go run ./cmd/capture-compatibility -url http://localhost:8082 -topic orders -out compatibility/captured/confluent-producer-edge-cases.json

prepare-benchmark-pages:
	@if [ -f dist/benchmark-regression.html ]; then \
		sh scripts/prepare-benchmark-pages.sh dist/benchmark-regression.html public; \
	elif [ -f dist/benchmark-comparison.html ]; then \
		sh scripts/prepare-benchmark-pages.sh dist/benchmark-comparison.html public; \
	else \
		echo "no benchmark report found; run make bench-regression first" >&2; \
		exit 1; \
	fi

otel-metrics:
	curl -fsS http://localhost:8080/metrics | sed -n '1,80p'
