package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(nil)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}

	if opts.URL != "http://localhost:8080" {
		t.Fatalf("URL = %q", opts.URL)
	}
	if opts.Topic != "orders" {
		t.Fatalf("Topic = %q", opts.Topic)
	}
	if opts.Duration != 10*time.Minute {
		t.Fatalf("Duration = %s", opts.Duration)
	}
	if opts.Clients != 32 {
		t.Fatalf("Clients = %d", opts.Clients)
	}
	if opts.RecordsPerRequest != 10 {
		t.Fatalf("RecordsPerRequest = %d", opts.RecordsPerRequest)
	}
	if opts.Format != "json" {
		t.Fatalf("Format = %q", opts.Format)
	}
	if opts.Thresholds.MaxFailureRate != 0 {
		t.Fatalf("MaxFailureRate = %f", opts.Thresholds.MaxFailureRate)
	}
}

func TestParseOptionsCustomValues(t *testing.T) {
	opts, err := parseOptions([]string{
		"-url", "http://proxy.example:18080/",
		"-topic", "payments",
		"-duration", "30s",
		"-warmup", "5s",
		"-clients", "4",
		"-records-per-request", "100",
		"-payload-bytes", "1024",
		"-format", "bin",
		"-timeout", "2s",
		"-max-latency-samples", "1000",
		"-max-failure-rate", "0.1%",
		"-min-records-sec", "50000",
		"-max-p99", "25ms",
	})
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}

	if opts.URL != "http://proxy.example:18080" {
		t.Fatalf("URL = %q", opts.URL)
	}
	if opts.Topic != "payments" || opts.Duration != 30*time.Second || opts.Warmup != 5*time.Second {
		t.Fatalf("unexpected topic/durations: %+v", opts)
	}
	if opts.Clients != 4 || opts.RecordsPerRequest != 100 || opts.PayloadBytes != 1024 {
		t.Fatalf("unexpected load options: %+v", opts)
	}
	if opts.Format != "binary" || opts.Timeout != 2*time.Second || opts.MaxLatencySamples != 1000 {
		t.Fatalf("unexpected format/timeout/sample options: %+v", opts)
	}
	if opts.Thresholds.MaxFailureRate != 0.001 {
		t.Fatalf("MaxFailureRate = %f", opts.Thresholds.MaxFailureRate)
	}
	if opts.Thresholds.MinRecordsPerSecond != 50000 || opts.Thresholds.MaxP99 != 25*time.Millisecond {
		t.Fatalf("unexpected thresholds: %+v", opts.Thresholds)
	}
}

func TestParseOptionsRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "relative url", args: []string{"-url", "localhost:8080"}, want: "absolute URL"},
		{name: "empty topic", args: []string{"-topic", " "}, want: "topic"},
		{name: "zero duration", args: []string{"-duration", "0"}, want: "duration"},
		{name: "negative warmup", args: []string{"-warmup", "-1s"}, want: "warmup"},
		{name: "zero clients", args: []string{"-clients", "0"}, want: "clients"},
		{name: "bad format", args: []string{"-format", "avro"}, want: "format"},
		{name: "bad failure rate", args: []string{"-max-failure-rate", "120%"}, want: "max-failure-rate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseOptions(tt.args)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}

func TestParseFailureRate(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{input: "0", want: 0},
		{input: "0.001", want: 0.001},
		{input: "0.1%", want: 0.001},
		{input: "100%", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseFailureRate(tt.input)
			if err != nil {
				t.Fatalf("parseFailureRate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %f, want %f", got, tt.want)
			}
		})
	}
}

func TestEvaluateThresholdsPasses(t *testing.T) {
	result := summary{
		RecordsPerSecond:  125000,
		RecordFailureRate: 0.0001,
		P99:               20 * time.Millisecond,
	}
	violations := evaluateThresholds(result, thresholds{
		MaxFailureRate:      0.001,
		MinRecordsPerSecond: 100000,
		MaxP99:              50 * time.Millisecond,
	})
	if len(violations) != 0 {
		t.Fatalf("unexpected violations: %+v", violations)
	}
}

func TestEvaluateThresholdsReportsAllFailures(t *testing.T) {
	result := summary{
		RecordsPerSecond:  90000,
		RecordFailureRate: 0.01,
		P99:               75 * time.Millisecond,
	}
	violations := evaluateThresholds(result, thresholds{
		MaxFailureRate:      0.001,
		MinRecordsPerSecond: 100000,
		MaxP99:              50 * time.Millisecond,
	})
	if len(violations) != 3 {
		t.Fatalf("violations = %+v", violations)
	}

	names := map[string]bool{}
	for _, violation := range violations {
		names[violation.Name] = true
	}
	for _, name := range []string{"max_failure_rate", "min_records_sec", "max_p99"} {
		if !names[name] {
			t.Fatalf("missing violation %s in %+v", name, violations)
		}
	}
}

func TestCountRecordResults(t *testing.T) {
	body := []byte(`{"offsets":[{"partition":0,"offset":10,"error_code":null,"error":null},{"partition":0,"offset":null,"error_code":50002,"error":"boom"}]}`)
	success, failed, err := countRecordResults(body, 2)
	if err != nil {
		t.Fatalf("countRecordResults: %v", err)
	}
	if success != 1 || failed != 1 {
		t.Fatalf("success=%d failed=%d", success, failed)
	}
}

func TestCountRecordResultsRejectsBadShape(t *testing.T) {
	_, _, err := countRecordResults([]byte(`{"offsets":[]}`), 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "offsets length") {
		t.Fatalf("unexpected error: %v", err)
	}
}
