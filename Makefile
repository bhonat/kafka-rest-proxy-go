.PHONY: test build run tidy docker-build compose-up compose-up-cluster compose-down compose-down-cluster compose-smoke

test:
	go test ./...

build:
	mkdir -p bin
	go build -o bin/kafka-rest-proxy-go ./cmd/kafka-rest-proxy-go

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
