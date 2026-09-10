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
	"context"
	"crypto/aes"
	"crypto/rand"
	"sort"
	"strings"
	"testing"

	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
)

var watchTestScheme = runtime.NewScheme()
var watchTestCodecs = serializer.NewCodecFactory(watchTestScheme)

func init() {
	metav1.AddToGroupVersion(watchTestScheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(watchTestScheme))
	utilruntime.Must(examplev1.AddToScheme(watchTestScheme))
}

// newAEADTransformer returns a real AES-GCM transformer.  AES-GCM authenticates
// the value.Context bytes (the additional authenticated data), so decrypting
// with a different context fails with "cipher: message authentication failed" --
// exactly the production symptom this package regressed on.
func newAEADTransformer(t *testing.T) value.Transformer {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generating AES key: %v", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("creating AES cipher: %v", err)
	}
	tr, err := aestransformer.NewGCMTransformer(block)
	if err != nil {
		t.Fatalf("creating GCM transformer: %v", err)
	}
	return tr
}

// newTestWatchChan builds a watchChan wired for decode-path testing.  watchKey
// is the watch's prefix, mirroring store.Watch, which always passes the
// /registry-qualified prefix (recursive for every informer).
func newTestWatchChan(t *testing.T, tr value.Transformer, watchKey string) *watchChan {
	t.Helper()
	w := &watcher{
		codec:     apitesting.TestCodec(watchTestCodecs, examplev1.SchemeGroupVersion),
		versioner: APIObjectVersioner,
		newFunc:   func() runtime.Object { return &example.Pod{} },
		// transformer is the field the decode path exercises.
		transformer: tr,
	}
	wc := &watchChan{
		watcher:   w,
		key:       []byte(watchKey),
		keyEnd:    prefixEnd([]byte(watchKey)),
		recursive: true,
		pred:      storage.Everything,
		resultCh:  make(chan watch.Event, outgoingBufSize),
		errCh:     make(chan error, 1),
	}
	wc.ctx, wc.cancel = context.WithCancel(context.Background())
	t.Cleanup(wc.cancel)
	return wc
}

// storeValue encodes obj the way store.Create does: codec-encode, prepend the
// rev header, then transform with the object's OWN full key as the AAD.
func storeValue(t *testing.T, wc *watchChan, tr value.Transformer, fullKey string, rev uint64, obj runtime.Object) []byte {
	t.Helper()
	encoded, err := runtime.Encode(wc.watcher.codec, obj)
	if err != nil {
		t.Fatalf("encoding object: %v", err)
	}
	wrapped := encodeWithRev(rev, encoded)
	out, err := tr.TransformToStorage(context.Background(), wrapped, authenticatedDataString([]byte(fullKey)))
	if err != nil {
		t.Fatalf("transforming to storage: %v", err)
	}
	return out
}

// TestWatchDecodeUsesPerObjectKeyAsAAD is the regression test for the bug that
// left the apiserver's Secret watch cache permanently empty.
//
// store.Create encrypts under the object's own prepared key, but the watcher
// used to decrypt every scanned object with wc.key -- the WATCH's prefix.  For
// a recursive watch (which is what every informer opens) the two differ, so
// AES-GCM rejected the ciphertext, every event was dropped as a "decode error"
// at V(4), and the cache for encrypted resources never populated.  Direct
// Get/GetList were unaffected because they pass the per-object key, which is
// why a quorum read returned the object while resourceVersion=0 returned none.
func TestWatchDecodeUsesPerObjectKeyAsAAD(t *testing.T) {
	tr := newAEADTransformer(t)
	const watchPrefix = "/registry/pods/"
	const fullKey = "/registry/pods/kube-system/bootstrap-pod"
	const rev = uint64(12345)

	wc := newTestWatchChan(t, tr, watchPrefix)
	pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bootstrap-pod", Namespace: "kube-system"}}
	stored := storeValue(t, wc, tr, fullKey, rev, pod)

	// The fix: decoding with the object's own key succeeds.
	obj, err := wc.decode([]byte(fullKey), stored, rev)
	if err != nil {
		t.Fatalf("decode with per-object key failed: %v", err)
	}
	got, ok := obj.(*example.Pod)
	if !ok {
		t.Fatalf("decode returned %T, want *example.Pod", obj)
	}
	if got.Name != "bootstrap-pod" {
		t.Errorf("decoded Name = %q, want %q", got.Name, "bootstrap-pod")
	}
	if got.ResourceVersion != "12345" {
		t.Errorf("decoded ResourceVersion = %q, want %q", got.ResourceVersion, "12345")
	}

	// Guard: decoding with the watch prefix (the old behaviour) must fail.
	// Without this the test would still pass if someone reintroduced the bug
	// alongside a transformer that ignores its context.
	if _, err := wc.decode([]byte(watchPrefix), stored, rev); err == nil {
		t.Fatal("decode with the watch prefix as AAD unexpectedly succeeded; " +
			"the AAD is not being authenticated, so this test cannot detect the regression")
	} else if !strings.Contains(err.Error(), "message authentication failed") {
		t.Logf("note: decode with wrong AAD failed with %q", err)
	}
}

// fakeIter is an in-memory scanIterator over a sorted key/value set.
type fakeIter struct {
	keys []string
	vals map[string][]byte
	i    int
}

func newFakeIter(vals map[string][]byte) *fakeIter {
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &fakeIter{keys: keys, vals: vals}
}

func (f *fakeIter) Valid() bool   { return f.i < len(f.keys) }
func (f *fakeIter) Key() []byte   { return []byte(f.keys[f.i]) }
func (f *fakeIter) Value() []byte { return f.vals[f.keys[f.i]] }
func (f *fakeIter) Next() error   { f.i++; return nil }
func (f *fakeIter) Close()        {}

// TestWatchEmitsEventsForEncryptedObjects drives the real seed and poll paths
// (scanAndEmit / pollDiff) over a recursive watch of encrypted objects.  This
// covers the call sites, not just decode itself: reverting any of them to
// wc.key silently drops every event and fails this test.
func TestWatchEmitsEventsForEncryptedObjects(t *testing.T) {
	tr := newAEADTransformer(t)
	const watchPrefix = "/registry/pods/"
	keyA := watchPrefix + "kube-system/pod-a"
	keyB := watchPrefix + "kube-system/pod-b"

	wc := newTestWatchChan(t, tr, watchPrefix)

	podA := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "kube-system"}}
	podB := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-b", Namespace: "kube-system"}}

	contents := map[string][]byte{
		keyA: storeValue(t, wc, tr, keyA, 100, podA),
		keyB: storeValue(t, wc, tr, keyB, 101, podB),
	}
	wc.newIter = func(ts uint64) (scanIterator, error) { return newFakeIter(contents), nil }

	// Seed with SendInitialEvents: both objects must surface as Added.
	fps, err := wc.scanAndEmit(1, true)
	if err != nil {
		t.Fatalf("scanAndEmit: %v", err)
	}
	if len(fps) != 2 {
		t.Fatalf("fingerprint count = %d, want 2", len(fps))
	}
	names := drainNames(t, wc, watch.Added, 2)
	if names[0] != "pod-a" || names[1] != "pod-b" {
		t.Errorf("initial Added events = %v, want [pod-a pod-b]", names)
	}

	wc.prevFP = fps
	wc.prevTS = 1

	// Modify pod-a; pollDiff must emit exactly one Modified event carrying the
	// decrypted object.
	podA2 := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "kube-system", Labels: map[string]string{"v": "2"}},
	}
	contents[keyA] = storeValue(t, wc, tr, keyA, 200, podA2)

	if _, err := wc.pollDiff(2); err != nil {
		t.Fatalf("pollDiff: %v", err)
	}
	modified := drainNames(t, wc, watch.Modified, 1)
	if modified[0] != "pod-a" {
		t.Errorf("Modified event = %v, want [pod-a]", modified)
	}
}

// drainNames reads exactly want events, asserts their type, and returns their
// sorted object names.
func drainNames(t *testing.T, wc *watchChan, typ watch.EventType, want int) []string {
	t.Helper()
	names := make([]string, 0, want)
	for i := 0; i < want; i++ {
		select {
		case ev := <-wc.resultCh:
			if ev.Type != typ {
				t.Fatalf("event %d type = %v, want %v", i, ev.Type, typ)
			}
			pod, ok := ev.Object.(*example.Pod)
			if !ok {
				t.Fatalf("event %d object = %T, want *example.Pod", i, ev.Object)
			}
			names = append(names, pod.Name)
		default:
			t.Fatalf("expected %d %v event(s), got only %d; "+
				"objects encrypted under their own key were not decryptable by the watcher", want, typ, i)
		}
	}
	select {
	case ev := <-wc.resultCh:
		t.Fatalf("unexpected extra event: %v %#v", ev.Type, ev.Object)
	default:
	}
	sort.Strings(names)
	return names
}
