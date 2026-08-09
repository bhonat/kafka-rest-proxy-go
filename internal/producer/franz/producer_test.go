package franz

import (
	"errors"
	"testing"

	"github.com/bhonat/kafka-rest-proxy-go/internal/limits"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRequestStateReleaseIsIdempotentAfterTimeoutThenCallbacks(t *testing.T) {
	admit := limits.NewAdmission(10, 100)
	if !admit.TryAcquire(2, 20) {
		t.Fatal("initial admission acquire failed")
	}

	state := newRequestState(2, admit, 2, 20)

	// Simulate the HTTP request timing out before franz-go callbacks complete.
	state.release()
	usedRecords, _, usedBytes, _ := admit.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("after timeout release admission = records %d bytes %d, want 0/0", usedRecords, usedBytes)
	}

	state.complete(0, &kgo.Record{Partition: 1, Offset: 42}, nil)
	state.complete(1, &kgo.Record{Partition: 2, Offset: 43}, errors.New("invalid partition"))

	select {
	case <-state.done:
	default:
		t.Fatal("state did not complete after all callbacks")
	}

	usedRecords, _, usedBytes, _ = admit.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("after callbacks admission = records %d bytes %d, want 0/0", usedRecords, usedBytes)
	}
	if state.results[0].Err != nil || state.results[0].Partition != 1 || state.results[0].Offset != 42 {
		t.Fatalf("success result = %#v", state.results[0])
	}
	if state.results[1].Err == nil {
		t.Fatalf("failure result err = nil")
	}
	if state.results[1].ErrorCode == nil || *state.results[1].ErrorCode != 50002 {
		t.Fatalf("failure error code = %#v, want 50002", state.results[1].ErrorCode)
	}
}
