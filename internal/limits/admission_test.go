package limits

import "testing"

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
