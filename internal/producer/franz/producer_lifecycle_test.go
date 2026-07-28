package franz

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/kafka-rest-proxy-go/internal/limits"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRequestStateReleasesAdmissionAfterAllCallbacks(t *testing.T) {
	admit := limits.NewAdmission(10, 100)
	if !admit.TryAcquire(2, 12) {
		t.Fatal("initial admission acquire failed")
	}

	state := newRequestState(2, admit, 2, 12)
	state.complete(1, &kgo.Record{Partition: 3, Offset: 101}, nil)

	select {
	case <-state.done:
		t.Fatal("request completed before all callbacks arrived")
	default:
	}

	usedRecords, _, usedBytes, _ := admit.Snapshot()
	if usedRecords != 2 || usedBytes != 12 {
		t.Fatalf("admission released early: records=%d bytes=%d", usedRecords, usedBytes)
	}

	state.complete(0, &kgo.Record{Partition: 2, Offset: 100}, nil)

	select {
	case <-state.done:
	case <-time.After(time.Second):
		t.Fatal("request did not complete after final callback")
	}

	usedRecords, _, usedBytes, _ = admit.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("admission leaked after callbacks: records=%d bytes=%d", usedRecords, usedBytes)
	}
	if state.results[0].Partition != 2 || state.results[0].Offset != 100 {
		t.Fatalf("result[0] = %#v", state.results[0])
	}
	if state.results[1].Partition != 3 || state.results[1].Offset != 101 {
		t.Fatalf("result[1] = %#v", state.results[1])
	}
}

func TestRequestStateReleaseIsIdempotentAfterContextCancellation(t *testing.T) {
	admit := limits.NewAdmission(10, 100)
	if !admit.TryAcquire(2, 20) {
		t.Fatal("initial admission acquire failed")
	}

	state := newRequestState(2, admit, 2, 20)
	state.release()
	state.release()

	usedRecords, _, usedBytes, _ := admit.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("admission should be released on cancellation: records=%d bytes=%d", usedRecords, usedBytes)
	}

	state.complete(0, &kgo.Record{}, errors.New("late callback after cancellation"))
	state.complete(1, &kgo.Record{}, errors.New("late callback after cancellation"))

	select {
	case <-state.done:
	case <-time.After(time.Second):
		t.Fatal("late callbacks did not finish request state")
	}

	usedRecords, _, usedBytes, _ = admit.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("late callbacks double-released admission: records=%d bytes=%d", usedRecords, usedBytes)
	}
}

func TestRequestStateConcurrentCallbacksReleaseAdmissionOnce(t *testing.T) {
	const n = 128
	admit := limits.NewAdmission(n, 4096)
	if !admit.TryAcquire(n, 4096) {
		t.Fatal("initial admission acquire failed")
	}

	state := newRequestState(n, admit, n, 4096)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			state.complete(i, &kgo.Record{Partition: int32(i % 11), Offset: int64(1000 + i)}, nil)
		}()
	}

	callbacksDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(callbacksDone)
	}()

	select {
	case <-callbacksDone:
	case <-time.After(time.Second):
		t.Fatal("callbacks did not finish")
	}
	select {
	case <-state.done:
	case <-time.After(time.Second):
		t.Fatal("request state did not close done")
	}

	usedRecords, _, usedBytes, _ := admit.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("admission leaked after concurrent callbacks: records=%d bytes=%d", usedRecords, usedBytes)
	}
	for i, result := range state.results {
		if result.Partition != int32(i%11) || result.Offset != int64(1000+i) {
			t.Fatalf("result[%d] = %#v", i, result)
		}
	}
}
