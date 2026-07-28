.PHONY: test test-race test-integration test-failure-integration test-differential test-security-integration test-hardening test-soak build run tidy docker-build compose-up compose-up-cluster compose-up-comparison compose-up-security compose-down compose-down-cluster compose-down-security compose-smoke bench-produce bench-html bench-suite bench-compare capture-confluent-fixtures otel-metrics

test:
	go test ./...

test-race:
	go test -race ./internal/api ./internal/limits ./internal/producer/franz ./compatibility ./cmd/bench-produce ./cmd/soak-produce

test-integration:
	KAFKA_INTEGRATION=1 go test ./integration -v

test-failure-integration:
	KAFKA_INTEGRATION=1 KAFKA_FAILURE_INTEGRATION=1 go test ./integration -run TestComposeKafkaFailureRecovery -count=1 -v

test-differential:
	KAFKA_REST_DIFFERENTIAL=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v

test-security-integration:
	KAFKA_SECURITY_INTEGRATION=1 go test ./integration -run TestSecurityIntegration -count=1 -v

test-hardening: test test-race test-integration test-differential

test-soak:
	go run ./cmd/soak-produce -url http://localhost:8080 -topic orders -duration 2m -warmup 10s -clients 16 -records-per-request 10 -payload-bytes 128 -format json -max-failure-rate 0 -min-records-sec 1000 -max-p99 250ms

build:
	mkdir -p bin
	go build -o bin/kafka-rest-proxy-go ./cmd/kafka-rest-proxy-go
	go build -o bin/bench-produce ./cmd/bench-produce
	go build -o bin/soak-produce ./cmd/soak-produce

run:
	go run ./cmd/kafka-rest-proxy-go

tidy:
	go mod tidy

docker-build:
	docker build -t kafka-rest-proxy-go:dev .

compose-up:
	docker compose up --build

compose-up-cluster:
	docker compose -f docker-compose.cluster.yml up --build

compose-up-comparison:
	docker compose --profile comparison up --build

compose-up-security:
	docker compose -f docker-compose.security.yml up --build

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

capture-confluent-fixtures:
	go run ./cmd/capture-compatibility -url http://localhost:8082 -topic orders -out compatibility/captured/confluent-producer-edge-cases.json

otel-metrics:
	curl -fsS http://localhost:8080/metrics | sed -n '1,80p'
