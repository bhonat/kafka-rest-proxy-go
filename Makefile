.PHONY: test test-integration build run tidy docker-build compose-up compose-up-cluster compose-up-comparison compose-down compose-down-cluster compose-smoke bench-produce bench-html bench-suite bench-compare capture-confluent-fixtures otel-metrics

test:
	go test ./...

test-integration:
	KAFKA_INTEGRATION=1 go test ./integration -v

build:
	mkdir -p bin
	go build -o bin/kafka-rest-proxy-go ./cmd/kafka-rest-proxy-go
	go build -o bin/bench-produce ./cmd/bench-produce

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

compose-down:
	docker compose down

compose-down-cluster:
	docker compose -f docker-compose.cluster.yml down

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
