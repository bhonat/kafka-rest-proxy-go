FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/kafka-rest-proxy-go ./cmd/kafka-rest-proxy-go

FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
LABEL org.opencontainers.image.title="KafkaRestProxy-Go" \
      org.opencontainers.image.description="Producer-only Confluent-compatible Kafka REST proxy in Go" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=build /out/kafka-rest-proxy-go /kafka-rest-proxy-go
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/kafka-rest-proxy-go"]
