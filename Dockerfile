FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kafka-rest-proxy-go ./cmd/kafka-rest-proxy-go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kafka-rest-proxy-go /kafka-rest-proxy-go
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/kafka-rest-proxy-go"]
