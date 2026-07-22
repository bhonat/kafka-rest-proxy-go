package main

import (
	"testing"
	"time"
)

func TestParseByteSizeList(t *testing.T) {
	got, err := parseByteSizeList("128,512,1KB,10KiB,1MiB")
	if err != nil {
		t.Fatal(err)
	}
	want := []int{128, 512, 1000, 10 * 1024, 1024 * 1024}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestBuildOptionsWithTargetsAndSuite(t *testing.T) {
	var targets targetFlags
	if err := targets.Set("go=http://localhost:8080"); err != nil {
		t.Fatal(err)
	}

	opts, err := buildOptions(
		"http://ignored:8080",
		"http://localhost:8082",
		targets,
		"orders",
		time.Second,
		10,
		5*time.Second,
		1000,
		"",
		"",
		true,
		512,
		10,
		4,
		"json",
		"128,512",
		"1,10",
		"4,8",
		"json",
		"runtime",
		"runtime",
		"runtime",
		"runtime",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.Targets) != 2 {
		t.Fatalf("targets = %d, want 2", len(opts.Targets))
	}
	if opts.Targets[0].Name != "go" || opts.Targets[1].Name != "confluent" {
		t.Fatalf("unexpected targets: %#v", opts.Targets)
	}
	if len(opts.Scenarios) != 8 {
		t.Fatalf("scenarios = %d, want 8", len(opts.Scenarios))
	}
}

func TestBuildOptionsWithFormatSuiteDimension(t *testing.T) {
	opts, err := buildOptions(
		"http://localhost:8080",
		"",
		nil,
		"orders",
		time.Second,
		10,
		5*time.Second,
		1000,
		"",
		"",
		true,
		512,
		10,
		4,
		"json",
		"128",
		"10",
		"4",
		"json,binary",
		"runtime",
		"runtime",
		"runtime",
		"runtime",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.Scenarios) != 2 {
		t.Fatalf("scenarios = %d, want 2", len(opts.Scenarios))
	}
	if opts.Scenarios[0].Format != "json" || opts.Scenarios[1].Format != "binary" {
		t.Fatalf("unexpected formats: %#v", opts.Scenarios)
	}
}

func TestBuildBinaryBodyUsesBase64StringValue(t *testing.T) {
	body, err := buildBody(1, 4, "key", "binary")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"records":[{"key":"a2V5LTA=","value":"eHh4eA=="}]}`
	if string(body) != want {
		t.Fatalf("body = %s, want %s", body, want)
	}
}

func TestParseFormatRejectsUnknown(t *testing.T) {
	if _, err := parseFormat("avro"); err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildComparisonsChoosesHighestRecordsPerSecond(t *testing.T) {
	rows := buildComparisons([]benchResult{
		{Scenario: "s1", TargetName: "go", RecordsPerSecond: 10},
		{Scenario: "s1", TargetName: "confluent", RecordsPerSecond: 8},
		{Scenario: "s2", TargetName: "go", RecordsPerSecond: 1},
		{Scenario: "s2", TargetName: "confluent", RecordsPerSecond: 2},
	})
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].Winner != "go" {
		t.Fatalf("rows[0].Winner = %q, want go", rows[0].Winner)
	}
	if rows[1].Winner != "confluent" {
		t.Fatalf("rows[1].Winner = %q, want confluent", rows[1].Winner)
	}
}
