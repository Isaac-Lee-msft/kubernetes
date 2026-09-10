/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tikv

import (
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// withCollectionQuota temporarily sets the package-level quota and restores it.
func withCollectionQuota(t *testing.T, n int64) {
	t.Helper()
	old := maxCollectionBytes
	maxCollectionBytes = n
	t.Cleanup(func() { maxCollectionBytes = old })
}

func newQuotaStore(seed int64) *store {
	s := &store{groupResource: schema.GroupResource{Resource: "configmaps"}}
	s.size.resync(seed)
	return s
}

func TestAdmitWriteUnderQuota(t *testing.T) {
	withCollectionQuota(t, 1000)
	s := newQuotaStore(500)
	if err := s.admitWrite("/registry/configmaps/a", 100, 0); err != nil {
		t.Fatalf("write that stays under quota was rejected: %v", err)
	}
}

func TestAdmitWriteRejectsGrowthPastQuota(t *testing.T) {
	withCollectionQuota(t, 1000)
	s := newQuotaStore(950)

	err := s.admitWrite("/registry/configmaps/a", 100, 0)
	if err == nil {
		t.Fatal("write past the collection quota was admitted")
	}
	// Must be a clean, non-retriable client error, not a 500: the client
	// cannot fix this by retrying, and a 5xx would make controllers hot-loop.
	if !apierrors.IsRequestEntityTooLargeError(err) {
		t.Errorf("error = %T (%v), want RequestEntityTooLarge", err, err)
	}
}

// A collection sitting exactly at quota must still accept writes that do not
// grow it, otherwise an operator has no way to rewrite or shrink objects and
// the only escape is blind deletion.
func TestAdmitWriteAllowsNonGrowingWriteAtQuota(t *testing.T) {
	withCollectionQuota(t, 1000)
	s := newQuotaStore(1000)

	if err := s.admitWrite("/registry/configmaps/a", 200, 200); err != nil {
		t.Errorf("same-size rewrite at quota was rejected: %v", err)
	}
	if err := s.admitWrite("/registry/configmaps/a", 50, 200); err != nil {
		t.Errorf("shrinking rewrite at quota was rejected: %v", err)
	}
	// ...but growing at quota must still fail.
	if err := s.admitWrite("/registry/configmaps/a", 400, 200); err == nil {
		t.Error("growing rewrite at quota was admitted")
	}
}

func TestAdmitWriteDisabled(t *testing.T) {
	withCollectionQuota(t, 0)
	s := newQuotaStore(1 << 40)
	if err := s.admitWrite("/registry/configmaps/a", 1<<30, 0); err != nil {
		t.Errorf("quota disabled (0) should admit everything, got: %v", err)
	}
}

// An update that replaces a large object with a small one must free budget, so
// a collection can recover from being at quota.
func TestNoteWriteAccounting(t *testing.T) {
	withCollectionQuota(t, 1000)
	s := newQuotaStore(0)

	s.noteWrite(300) // create
	s.noteWrite(200) // create
	if got := s.size.get(); got != 500 {
		t.Fatalf("after two creates size = %d, want 500", got)
	}
	s.noteWrite(-300) // delete
	if got := s.size.get(); got != 200 {
		t.Fatalf("after delete size = %d, want 200", got)
	}
	s.noteWrite(50 - 200) // update shrinking 200 -> 50
	if got := s.size.get(); got != 50 {
		t.Fatalf("after shrinking update size = %d, want 50", got)
	}
}

// Drift (e.g. deletes seen by another replica) must never drive the counter
// negative, which would make the quota check meaninglessly permissive.
func TestCollectionSizeClampsAtZero(t *testing.T) {
	s := newQuotaStore(100)
	s.noteWrite(-500)
	if got := s.size.get(); got != 0 {
		t.Fatalf("size = %d, want clamped to 0", got)
	}
}

// A full unpaginated LIST is authoritative and must overwrite accumulated
// drift in both directions.
func TestResyncOverwritesDrift(t *testing.T) {
	s := newQuotaStore(0)
	s.noteWrite(10_000) // local writes only
	s.size.resync(42)   // relist says the truth is much smaller
	if got := s.size.get(); got != 42 {
		t.Fatalf("after resync size = %d, want 42", got)
	}
	s.size.resync(999_999)
	if got := s.size.get(); got != 999_999 {
		t.Fatalf("after second resync size = %d, want 999999", got)
	}
}

// The default must tie to listMaxBytes: a collection that cannot be LISTed
// past N bytes must not be allowed to grow past N bytes either, or the cluster
// can write itself into a state where every relist fails.
func TestDefaultQuotaTracksListMaxBytes(t *testing.T) {
	if maxCollectionBytes != listMaxBytes {
		t.Errorf("maxCollectionBytes = %d, want it to default to listMaxBytes = %d",
			maxCollectionBytes, listMaxBytes)
	}
}
