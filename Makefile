.PHONY: test test-integration build run tidy docker-build compose-up compose-up-cluster compose-down compose-down-cluster compose-smoke bench-produce

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

compose-down:
	docker compose down

compose-down-cluster:
	docker compose -f docker-compose.cluster.yml down

compose-smoke:
	scripts/compose-smoke.sh

bench-produce:
	go run ./cmd/bench-produce -url http://localhost:8080 -topic orders -duration 30s -clients 32 -records 10 -payload-bytes 512
