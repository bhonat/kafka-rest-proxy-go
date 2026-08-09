package ratelimit

import (
	"sync"
	"time"
)

// Limiter is a small token-bucket rate limiter used for Confluent-style
// request-count and byte-cost rate limiting. It is deliberately non-blocking:
// callers either acquire capacity immediately or return HTTP 429.
type Limiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func New(ratePerSecond float64, burst int64) *Limiter {
	if ratePerSecond <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = 1
	}
	now := time.Now()
	return &Limiter{
		rate:   ratePerSecond,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   now,
	}
}

func (l *Limiter) Allow(cost int64) bool {
	if l == nil || cost <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = now
	}

	needed := float64(cost)
	if l.tokens < needed {
		return false
	}
	l.tokens -= needed
	return true
}
