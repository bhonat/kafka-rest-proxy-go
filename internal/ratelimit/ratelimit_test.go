package ratelimit

import (
	"testing"
	"time"
)

func TestLimiterAllowsBurstThenRejects(t *testing.T) {
	l := New(1, 2)
	if !l.Allow(1) {
		t.Fatal("first request rejected")
	}
	if !l.Allow(1) {
		t.Fatal("second request rejected")
	}
	if l.Allow(1) {
		t.Fatal("third request should be rejected")
	}
}

func TestLimiterRefills(t *testing.T) {
	l := New(100, 1)
	if !l.Allow(1) {
		t.Fatal("first request rejected")
	}
	if l.Allow(1) {
		t.Fatal("second request should be rejected")
	}
	time.Sleep(20 * time.Millisecond)
	if !l.Allow(1) {
		t.Fatal("request should be allowed after refill")
	}
}
