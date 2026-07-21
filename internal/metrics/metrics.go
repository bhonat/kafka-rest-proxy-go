package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Metrics struct {
	provider *sdkmetric.MeterProvider
	handler  http.Handler

	requests        metric.Int64Counter
	recordsAccepted metric.Int64Counter
	recordsProduced metric.Int64Counter
	recordsFailed   metric.Int64Counter
	requestBytes    metric.Int64Counter
	produceLatency  metric.Float64Histogram
	decodeLatency   metric.Float64Histogram
	callbackWait    metric.Float64Histogram
	rejections      metric.Int64Counter

	outstandingRequests atomic.Int64
	admissionSnapshot   func() (usedRecords, maxRecords, usedBytes, maxBytes int64)
	bufferedSnapshot    func() (records, bytes int64)
}

func New() (*Metrics, error) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, err
	}

	latencyBuckets := []float64{
		0.001,
		0.0025,
		0.005,
		0.01,
		0.025,
		0.05,
		0.1,
		0.25,
		0.5,
		1,
		2.5,
		5,
		10,
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(resource.NewSchemaless(
			attribute.String("service.name", "kafka-rest-proxy-go"),
		)),
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "kafka_rest_produce_latency"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBuckets,
			}},
		)),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "kafka_rest_decode_latency"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBuckets,
			}},
		)),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "kafka_rest_kafka_callback_wait_latency"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBuckets,
			}},
		)),
	)
	meter := provider.Meter("github.com/example/kafka-rest-proxy-go/internal/metrics")

	m := &Metrics{
		provider: provider,
		handler:  promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}

	if m.requests, err = meter.Int64Counter(
		"kafka_rest_requests",
		metric.WithDescription("Total HTTP produce requests."),
	); err != nil {
		return nil, err
	}
	if m.recordsAccepted, err = meter.Int64Counter(
		"kafka_rest_records_accepted",
		metric.WithDescription("Records accepted in successful HTTP requests."),
	); err != nil {
		return nil, err
	}
	if m.recordsProduced, err = meter.Int64Counter(
		"kafka_rest_records_produced",
		metric.WithDescription("Records successfully acknowledged by Kafka."),
	); err != nil {
		return nil, err
	}
	if m.recordsFailed, err = meter.Int64Counter(
		"kafka_rest_records_failed",
		metric.WithDescription("Records failed by Kafka/client callbacks."),
	); err != nil {
		return nil, err
	}
	if m.requestBytes, err = meter.Int64Counter(
		"kafka_rest_request_bytes",
		metric.WithDescription("HTTP request body bytes processed."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.produceLatency, err = meter.Float64Histogram(
		"kafka_rest_produce_latency",
		metric.WithDescription("End-to-end HTTP produce request latency."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.decodeLatency, err = meter.Float64Histogram(
		"kafka_rest_decode_latency",
		metric.WithDescription("Request body decode and validation latency."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.callbackWait, err = meter.Float64Histogram(
		"kafka_rest_kafka_callback_wait_latency",
		metric.WithDescription("Time spent waiting for Kafka producer callbacks for a request."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.rejections, err = meter.Int64Counter(
		"kafka_rest_admission_rejections",
		metric.WithDescription("Requests rejected because local producer admission capacity was exhausted."),
	); err != nil {
		return nil, err
	}

	if _, err = meter.Int64ObservableGauge(
		"kafka_rest_outstanding_requests",
		metric.WithDescription("Currently active HTTP requests."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.outstandingRequests.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err = meter.Int64ObservableGauge(
		"kafka_rest_admission_records",
		metric.WithDescription("Records reserved by local admission control."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			usedRecords, maxRecords, _, _ := m.admission()
			o.Observe(usedRecords, metric.WithAttributes(attribute.String("kind", "used")))
			o.Observe(maxRecords, metric.WithAttributes(attribute.String("kind", "limit")))
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err = meter.Int64ObservableGauge(
		"kafka_rest_admission_bytes",
		metric.WithDescription("Bytes reserved by local admission control."),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			_, _, usedBytes, maxBytes := m.admission()
			o.Observe(usedBytes, metric.WithAttributes(attribute.String("kind", "used")))
			o.Observe(maxBytes, metric.WithAttributes(attribute.String("kind", "limit")))
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err = meter.Int64ObservableGauge(
		"kafka_rest_franz_buffered_records",
		metric.WithDescription("Records currently buffered by franz-go."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			records, _ := m.buffered()
			o.Observe(records)
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err = meter.Int64ObservableGauge(
		"kafka_rest_franz_buffered_bytes",
		metric.WithDescription("Bytes currently buffered by franz-go."),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			_, bytes := m.buffered()
			o.Observe(bytes)
			return nil
		}),
	); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Metrics) SetAdmissionSnapshot(fn func() (usedRecords, maxRecords, usedBytes, maxBytes int64)) {
	m.admissionSnapshot = fn
}

func (m *Metrics) SetBufferedSnapshot(fn func() (records, bytes int64)) {
	m.bufferedSnapshot = fn
}

func (m *Metrics) ObserveRequest(status int, requestBytes int64, records int, duration time.Duration) {
	ctx := context.Background()
	m.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("status_class", statusClass(status))))
	m.requestBytes.Add(ctx, requestBytes)
	m.produceLatency.Record(ctx, duration.Seconds(), metric.WithAttributes(attribute.String("status_class", statusClass(status))))
	if records > 0 && status < http.StatusBadRequest {
		m.recordsAccepted.Add(ctx, int64(records))
	}
}

func (m *Metrics) ObserveProduceResult(successes, failures int) {
	ctx := context.Background()
	if successes > 0 {
		m.recordsProduced.Add(ctx, int64(successes))
	}
	if failures > 0 {
		m.recordsFailed.Add(ctx, int64(failures))
	}
}

func (m *Metrics) ObserveDecode(format string, success bool, duration time.Duration) {
	ctx := context.Background()
	m.decodeLatency.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("format", format),
		attribute.Bool("success", success),
	))
}

func (m *Metrics) ObserveKafkaCallbackWait(status int, duration time.Duration) {
	ctx := context.Background()
	m.callbackWait.Record(ctx, duration.Seconds(), metric.WithAttributes(attribute.String("status_class", statusClass(status))))
}

func (m *Metrics) ObserveAdmissionRejected() {
	m.rejections.Add(context.Background(), 1)
}

func (m *Metrics) IncOutstanding() {
	m.outstandingRequests.Add(1)
}

func (m *Metrics) DecOutstanding() {
	m.outstandingRequests.Add(-1)
}

func (m *Metrics) Handler() http.Handler {
	return m.handler
}

func (m *Metrics) Shutdown(ctx context.Context) error {
	if m.provider == nil {
		return nil
	}
	return m.provider.Shutdown(ctx)
}

func (m *Metrics) admission() (usedRecords, maxRecords, usedBytes, maxBytes int64) {
	if m.admissionSnapshot == nil {
		return 0, 0, 0, 0
	}
	return m.admissionSnapshot()
}

func (m *Metrics) buffered() (records, bytes int64) {
	if m.bufferedSnapshot == nil {
		return 0, 0
	}
	return m.bufferedSnapshot()
}

func statusClass(status int) string {
	if status <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%dxx", status/100)
}
