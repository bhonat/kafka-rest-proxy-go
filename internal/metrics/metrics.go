package metrics

import (
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type Metrics struct {
	requestsTotal       atomic.Int64
	requests2xx         atomic.Int64
	requests4xx         atomic.Int64
	requests5xx         atomic.Int64
	recordsAccepted     atomic.Int64
	recordsProduced     atomic.Int64
	recordsFailed       atomic.Int64
	requestBytesTotal   atomic.Int64
	produceLatencyNanos atomic.Int64
	produceLatencyCount atomic.Int64
	outstandingRequests atomic.Int64
	admissionSnapshot   func() (usedRecords, maxRecords, usedBytes, maxBytes int64)
	bufferedSnapshot    func() (records, bytes int64)
}

func New() *Metrics {
	return &Metrics{}
}

func (m *Metrics) SetAdmissionSnapshot(fn func() (usedRecords, maxRecords, usedBytes, maxBytes int64)) {
	m.admissionSnapshot = fn
}

func (m *Metrics) SetBufferedSnapshot(fn func() (records, bytes int64)) {
	m.bufferedSnapshot = fn
}

func (m *Metrics) ObserveRequest(status int, requestBytes int64, records int, duration time.Duration) {
	m.requestsTotal.Add(1)
	m.requestBytesTotal.Add(requestBytes)
	m.produceLatencyNanos.Add(duration.Nanoseconds())
	m.produceLatencyCount.Add(1)

	switch {
	case status >= 500:
		m.requests5xx.Add(1)
	case status >= 400:
		m.requests4xx.Add(1)
	default:
		m.requests2xx.Add(1)
	}
	if records > 0 && status < 400 {
		m.recordsAccepted.Add(int64(records))
	}
}

func (m *Metrics) ObserveProduceResult(successes, failures int) {
	if successes > 0 {
		m.recordsProduced.Add(int64(successes))
	}
	if failures > 0 {
		m.recordsFailed.Add(int64(failures))
	}
}

func (m *Metrics) IncOutstanding() {
	m.outstandingRequests.Add(1)
}

func (m *Metrics) DecOutstanding() {
	m.outstandingRequests.Add(-1)
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		writeMetric(w, "kafka_rest_requests_total", "Total HTTP requests.", m.requestsTotal.Load())
		writeMetric(w, "kafka_rest_requests_2xx_total", "HTTP requests returning 2xx/3xx.", m.requests2xx.Load())
		writeMetric(w, "kafka_rest_requests_4xx_total", "HTTP requests returning 4xx.", m.requests4xx.Load())
		writeMetric(w, "kafka_rest_requests_5xx_total", "HTTP requests returning 5xx.", m.requests5xx.Load())
		writeMetric(w, "kafka_rest_records_accepted_total", "Records accepted in successful HTTP requests.", m.recordsAccepted.Load())
		writeMetric(w, "kafka_rest_records_produced_total", "Records successfully acknowledged by Kafka.", m.recordsProduced.Load())
		writeMetric(w, "kafka_rest_records_failed_total", "Records failed by Kafka/client callbacks.", m.recordsFailed.Load())
		writeMetric(w, "kafka_rest_request_bytes_total", "HTTP request body bytes processed.", m.requestBytesTotal.Load())
		writeMetric(w, "kafka_rest_outstanding_requests", "Currently active HTTP requests.", m.outstandingRequests.Load())
		writeMetric(w, "kafka_rest_produce_latency_seconds_count", "Produce request latency observation count.", m.produceLatencyCount.Load())
		fmt.Fprintf(w, "# HELP kafka_rest_produce_latency_seconds_sum Sum of produce request latency in seconds.\n")
		fmt.Fprintf(w, "# TYPE kafka_rest_produce_latency_seconds_sum counter\n")
		fmt.Fprintf(w, "kafka_rest_produce_latency_seconds_sum %s\n", strconv.FormatFloat(float64(m.produceLatencyNanos.Load())/float64(time.Second), 'f', 9, 64))

		if m.admissionSnapshot != nil {
			usedRecords, maxRecords, usedBytes, maxBytes := m.admissionSnapshot()
			writeMetric(w, "kafka_rest_admission_records", "Records reserved by local admission control.", usedRecords)
			writeMetric(w, "kafka_rest_admission_records_limit", "Record admission capacity.", maxRecords)
			writeMetric(w, "kafka_rest_admission_bytes", "Bytes reserved by local admission control.", usedBytes)
			writeMetric(w, "kafka_rest_admission_bytes_limit", "Byte admission capacity.", maxBytes)
		}

		if m.bufferedSnapshot != nil {
			records, bytes := m.bufferedSnapshot()
			writeMetric(w, "kafka_rest_franz_buffered_records", "Records currently buffered by franz-go.", records)
			writeMetric(w, "kafka_rest_franz_buffered_bytes", "Bytes currently buffered by franz-go.", bytes)
		}
	})
}

func writeMetric(w http.ResponseWriter, name, help string, value int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	fmt.Fprintf(w, "%s %d\n", name, value)
}
