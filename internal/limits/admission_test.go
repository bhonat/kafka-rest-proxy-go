package limits

import (
	"context"
	"testing"
)

func TestAdmission(t *testing.T) {
	a := NewAdmission(2, 10)
	if !a.TryAcquire(1, 4) {
		t.Fatal("first acquire failed")
	}
	if a.TryAcquire(2, 1) {
		t.Fatal("record limit should reject")
	}
	if a.TryAcquire(1, 7) {
		t.Fatal("byte limit should reject")
	}
	a.Release(1, 4)
	if !a.TryAcquire(2, 10) {
		t.Fatal("acquire after release failed")
	}
}

func TestAdmissionPartialAndOverReleaseClampsAtZero(t *testing.T) {
	a := NewAdmission(10, 100)
	if !a.TryAcquire(4, 40) {
		t.Fatal("acquire failed")
	}

	a.Release(2, 15)
	usedRecords, _, usedBytes, _ := a.Snapshot()
	if usedRecords != 2 || usedBytes != 25 {
		t.Fatalf("snapshot after partial release = records=%d bytes=%d", usedRecords, usedBytes)
	}

	a.Release(100, 1000)
	usedRecords, _, usedBytes, _ = a.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("release should clamp at zero: records=%d bytes=%d", usedRecords, usedBytes)
	}

	a.Release(1, 1)
	usedRecords, _, usedBytes, _ = a.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("extra release should remain zero: records=%d bytes=%d", usedRecords, usedBytes)
	}
}

func TestAdmissionRejectsNegativeAcquireWithoutLeaking(t *testing.T) {
	a := NewAdmission(10, 100)
	if a.TryAcquire(-1, 1) {
		t.Fatal("negative records acquire should fail")
	}
	if a.TryAcquire(1, -1) {
		t.Fatal("negative bytes acquire should fail")
	}

	usedRecords, _, usedBytes, _ := a.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("negative acquire leaked capacity: records=%d bytes=%d", usedRecords, usedBytes)
	}
}

func TestAdmissionRejectsRecordCapWithoutConsumingCapacity(t *testing.T) {
	a := NewAdmission(2, 0)

	if !a.TryAcquire(2, 100) {
		t.Fatal("initial acquire failed")
	}
	if a.TryAcquire(1, 1) {
		t.Fatal("record cap should reject one additional record")
	}

	usedRecords, maxRecords, usedBytes, maxBytes := a.Snapshot()
	if usedRecords != 2 || maxRecords != 2 || usedBytes != 100 || maxBytes != 0 {
		t.Fatalf("snapshot after rejected acquire = (%d,%d,%d,%d), want (2,2,100,0)", usedRecords, maxRecords, usedBytes, maxBytes)
	}

	a.Release(2, 100)
	if !a.TryAcquire(2, 1) {
		t.Fatal("capacity was not released after record-cap rejection")
	}
}

func TestAdmissionRejectsByteCapWithoutConsumingCapacity(t *testing.T) {
	a := NewAdmission(0, 10)

	if !a.TryAcquire(1, 10) {
		t.Fatal("initial acquire failed")
	}
	if a.TryAcquire(1, 1) {
		t.Fatal("byte cap should reject one additional byte")
	}

	usedRecords, maxRecords, usedBytes, maxBytes := a.Snapshot()
	if usedRecords != 1 || maxRecords != 0 || usedBytes != 10 || maxBytes != 10 {
		t.Fatalf("snapshot after rejected acquire = (%d,%d,%d,%d), want (1,0,10,10)", usedRecords, maxRecords, usedBytes, maxBytes)
	}

	a.Release(1, 10)
	if !a.TryAcquire(100, 10) {
		t.Fatal("capacity was not released after byte-cap rejection")
	}
}

func TestAdmissionReleaseAfterSuccessFreesCapacity(t *testing.T) {
	a := NewAdmission(1, 5)

	if !a.TryAcquire(1, 5) {
		t.Fatal("initial acquire failed")
	}
	if a.TryAcquire(1, 1) {
		t.Fatal("capacity should be full before release")
	}

	a.Release(1, 5)

	if !a.TryAcquire(1, 5) {
		t.Fatal("release after successful completion did not free capacity")
	}
}

func TestAdmissionReleaseAfterContextCancellationFreesCapacity(t *testing.T) {
	a := NewAdmission(1, 5)
	ctx, cancel := context.WithCancel(context.Background())

	if !a.TryAcquire(1, 5) {
		t.Fatal("initial acquire failed")
	}
	cancel()
	<-ctx.Done()
	a.Release(1, 5)

	usedRecords, _, usedBytes, _ := a.Snapshot()
	if usedRecords != 0 || usedBytes != 0 {
		t.Fatalf("snapshot after cancellation release = (%d records, %d bytes), want zero", usedRecords, usedBytes)
	}
	if !a.TryAcquire(1, 5) {
		t.Fatal("release after context cancellation did not free capacity")
	}
}
