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
	"fmt"
	"hash/fnv"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/tikv/metrics"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
)

const (
	// incomingBufSize is the size of the internal event channel buffer.
	incomingBufSize = 100
	// outgoingBufSize is the size of the result channel buffer exposed to consumers.
	outgoingBufSize = 100
	// defaultPollInterval is the default MVCC-scan polling interval.
	defaultPollInterval = 250 * time.Millisecond
)

// fingerprint is the compact per-key state retained between polls.  Storing a
// content hash (not the full value) keeps a watch's steady-state memory
// proportional to the NUMBER of keys, not their total bytes: a watch over a
// large resource therefore does not hold a second full copy of the data on top
// of the one the watch cache already keeps.  A Spanner-style change feed
// retains no dataset at all; with MVCC polling this 8-byte-per-key fingerprint
// is the minimum needed to detect per-key changes between scans.
type fingerprint = uint64

// watcher creates watchChan instances for a given TiKV store.
type watcher struct {
	client        *txnkv.Client
	codec         runtime.Codec
	versioner     storage.Versioner
	transformer   value.Transformer
	newFunc       func() runtime.Object
	objectType    string
	groupResource schema.GroupResource
	pollInterval  time.Duration

	getCurrentStorageRV func(context.Context) (uint64, error)
}

// Watch starts a watch on key (or on the entire prefix when opts.Recursive is
// true).  Events are returned on the watch.Interface channel.
//
// Implementation strategy: MVCC polling (Strategy A from the design doc).
// A goroutine polls TiKV at pollInterval, diffs against the previous snapshot,
// and converts changes into watch.Events.  A CDC-based implementation can be
// substituted later without changing the public interface.
func (w *watcher) Watch(ctx context.Context, key string, rev int64, opts storage.ListOptions) (watch.Interface, error) {
	if opts.Recursive && key[len(key)-1] != '/' {
		key += "/"
	}
	if opts.ProgressNotify && w.newFunc == nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("progressNotify for watch is unsupported without a newFunc"))
	}

	var startTS uint64
	if rev > 0 {
		startTS = uint64(rev)
	} else {
		// Obtain current timestamp from PD.
		ts, err := w.client.GetTimestamp(ctx)
		if err != nil {
			return nil, fmt.Errorf("tikv watch: failed to get current timestamp: %w", err)
		}
		startTS = ts
	}

	wc := &watchChan{
		watcher:        w,
		key:            []byte(key),
		keyEnd:         prefixEnd([]byte(key)),
		recursive:      opts.Recursive,
		progressNotify: opts.ProgressNotify,
		pred:           opts.Predicate,
		startTS:        startTS,
		resultCh:       make(chan watch.Event, outgoingBufSize),
		errCh:          make(chan error, 1),
	}
	wc.ctx, wc.cancel = context.WithCancel(ctx)

	sendInitial := opts.SendInitialEvents != nil && *opts.SendInitialEvents
	metrics.IncWatchOpen()
	go wc.run(sendInitial)
	return wc, nil
}

// watchChan implements watch.Interface backed by periodic MVCC polling.
type watchChan struct {
	watcher        *watcher
	key            []byte
	keyEnd         []byte
	recursive      bool
	progressNotify bool
	pred           storage.SelectionPredicate
	startTS        uint64

	ctx    context.Context
	cancel context.CancelFunc

	resultCh chan watch.Event
	errCh    chan error

	// prevFP holds the last observed per-key fingerprints for diffing, and
	// prevTS is the snapshot timestamp at which they were read.  Together they
	// let a poll detect added/modified/deleted keys while retaining only a
	// hash per key; the old object needed for a Deleted (or predicate
	// transition) event is reconstructed on demand by a historical read at
	// prevTS, which the compactor's GC/history window guarantees is still
	// readable.
	prevFP map[string]fingerprint
	prevTS uint64
}

// ResultChan implements watch.Interface.
func (wc *watchChan) ResultChan() <-chan watch.Event {
	return wc.resultCh
}

// Stop implements watch.Interface.
func (wc *watchChan) Stop() {
	wc.cancel()
}

func (wc *watchChan) run(sendInitial bool) {
	defer func() {
		close(wc.resultCh)
		metrics.DecWatchOpen()
		klog.V(6).Infof("tikv watcher: stopped for key %s", wc.key)
	}()

	// Seed with a single bounded, streaming scan at the initial snapshot
	// timestamp.  wc.startTS was already pinned in Watch() to the caller's
	// resume revision, or to the current TSO when none was given, so all of
	// the seed -- and every later poll -- observes a consistent MVCC view.
	// scanAndEmit retains only an 8-byte fingerprint per key and, when this is
	// a SendInitialEvents (WatchList) seed, streams each matching object out as
	// an Added event while freeing its value immediately, so the seed never
	// materialises the whole resource in apiserver memory.
	initialTS := wc.startTS
	prevFP, err := wc.scanAndEmit(initialTS, sendInitial)
	if err != nil {
		wc.handleError(err)
		return
	}

	if sendInitial {
		// Emit the InitialEventsEnd bookmark required by the WatchList /
		// SendInitialEvents protocol.  It is mandatory (independent of
		// ProgressNotify) and must carry the `k8s.io/initial-events-end:
		// "true"` annotation.
		wc.sendInitialEventsEnd(initialTS)
	}

	wc.prevFP = prevFP
	wc.prevTS = initialTS

	ticker := time.NewTicker(wc.watcher.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wc.ctx.Done():
			return
		case <-ticker.C:
			newTS, err := wc.watcher.client.GetTimestamp(wc.ctx)
			if err != nil {
				if wc.ctx.Err() != nil {
					return
				}
				klog.V(4).Infof("tikv watcher: timestamp error: %v", err)
				continue
			}
			pollStart := time.Now()
			newFP, err := wc.pollDiff(newTS)
			if err != nil {
				if wc.ctx.Err() != nil {
					return
				}
				klog.V(4).Infof("tikv watcher: snapshot error: %v", err)
				continue
			}
			metrics.RecordWatchPoll(wc.watcher.groupResource, time.Since(pollStart), len(newFP))
			wc.prevFP = newFP
			wc.prevTS = newTS

			if wc.progressNotify && wc.watcher.newFunc != nil {
				bookmark := wc.watcher.newFunc()
				if err := wc.watcher.versioner.UpdateObject(bookmark, newTS); err == nil {
					wc.sendEvent(watch.Event{Type: watch.Bookmark, Object: bookmark})
					metrics.RecordWatchEvent("bookmark")
				}
			}
		}
	}
}

// scanIterator is the subset of the tikv client-go iterator used by the
// watcher.  It is named so helpers can return it (the concrete type lives in
// an internal client-go package that cannot be imported here).
type scanIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

// iterBounds returns the [start, end) key range for this watch.
func (wc *watchChan) iterBounds() ([]byte, []byte) {
	if wc.recursive {
		return wc.key, wc.keyEnd
	}
	return wc.key, append(cloneBytes(wc.key), 0x00)
}

// boundedSnapshotIter opens a bounded MVCC scan over the watch's key range at
// the given snapshot timestamp.  SetScanBatchSize caps the per-RPC key count
// (the scanner frees each batch as it advances) and SetNotFillCache avoids
// evicting hot keys from TiKV's block cache -- the same bounds GetList applies,
// so a watch poll over a large resource never balloons TiKV-side memory.
func (wc *watchChan) boundedSnapshotIter(ts uint64) (scanIterator, error) {
	snap := wc.watcher.client.GetSnapshot(ts)
	snap.SetScanBatchSize(listScanBatchSize)
	snap.SetNotFillCache(true)
	start, end := wc.iterBounds()
	return snap.Iter(start, end)
}

// hashValue returns a content fingerprint of a raw stored value.  Any write to
// a key changes its stored bytes (a fresh rev header is prepended on every
// update), so a changed hash reliably signals "this key changed since the last
// poll" without decoding or decrypting the value -- keeping the per-key,
// per-poll cost to a cheap hash over the still-batched bytes.
func hashValue(raw []byte) fingerprint {
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return h.Sum64()
}

// scanAndEmit performs one bounded streaming scan at ts and returns the per-key
// fingerprints.  When emitInitial is true it also streams a synthetic Added
// event for every matching object as it is scanned (the SendInitialEvents
// seed), decoding and releasing each value immediately so the seed never holds
// the whole resource in memory at once.
func (wc *watchChan) scanAndEmit(ts uint64, emitInitial bool) (map[string]fingerprint, error) {
	iter, err := wc.boundedSnapshotIter(ts)
	if err != nil {
		return nil, fmt.Errorf("tikv watcher: snapshot iter at ts=%d: %w", ts, err)
	}
	defer iter.Close()

	fps := make(map[string]fingerprint)
	for iter.Valid() {
		k := cloneBytes(iter.Key())
		rawVal := iter.Value() // batch-owned; consumed before iter.Next()
		fps[string(k)] = hashValue(rawVal)
		if emitInitial {
			if obj, derr := wc.decode(rawVal, ts); derr != nil {
				klog.V(4).Infof("tikv watcher: decode error during initial events for %s: %v", k, derr)
			} else if matched, merr := wc.pred.Matches(obj); merr == nil && matched {
				wc.sendEvent(watch.Event{Type: watch.Added, Object: obj})
				metrics.RecordWatchEvent("added")
			}
		}
		if err := iter.Next(); err != nil {
			break
		}
	}
	return fps, nil
}

// pollDiff performs one bounded streaming scan at newTS, diffs it against the
// retained fingerprints, and emits Added/Modified/Deleted events.  Added and
// Modified objects are decoded from the value in hand during the scan; the old
// object needed for a Deleted (or a predicate-filter transition) is
// reconstructed by a historical single-key read at prevTS.  Only the fingerprint
// map is retained, so steady-state memory is O(keys), not O(bytes).
func (wc *watchChan) pollDiff(newTS uint64) (map[string]fingerprint, error) {
	iter, err := wc.boundedSnapshotIter(newTS)
	if err != nil {
		return nil, fmt.Errorf("tikv watcher: iter: %w", err)
	}
	defer iter.Close()

	newFP := make(map[string]fingerprint, len(wc.prevFP))
	for iter.Valid() {
		k := cloneBytes(iter.Key())
		rawVal := iter.Value() // batch-owned; consumed before iter.Next()
		h := hashValue(rawVal)
		ks := string(k)
		newFP[ks] = h

		switch oldHash, existed := wc.prevFP[ks]; {
		case !existed:
			if obj, derr := wc.decode(rawVal, newTS); derr != nil {
				klog.V(4).Infof("tikv watcher: decode error for added key %s: %v", k, derr)
			} else if matched, _ := wc.pred.Matches(obj); matched {
				wc.sendEvent(watch.Event{Type: watch.Added, Object: obj})
				metrics.RecordWatchEvent("added")
			}
		case oldHash != h:
			wc.emitModified(k, rawVal, newTS)
		}
		if err := iter.Next(); err != nil {
			break
		}
	}

	// Deleted keys: present in the previous fingerprints, absent now.
	for ks := range wc.prevFP {
		if _, stillThere := newFP[ks]; !stillThere {
			wc.emitDeleted([]byte(ks))
		}
	}
	return newFP, nil
}

// emitModified handles a key whose stored value changed since the last poll.
// With the default (empty) predicate the new object is emitted as Modified
// directly.  With an active field/label selector the OLD object is
// reconstructed (historical read at prevTS) so predicate-filter transitions
// (enter -> Added, leave -> Deleted) are surfaced exactly as etcd would.
func (wc *watchChan) emitModified(key, rawVal []byte, newTS uint64) {
	newObj, err := wc.decode(rawVal, newTS)
	if err != nil {
		klog.V(4).Infof("tikv watcher: decode error for modified key %s: %v", key, err)
		return
	}
	if wc.pred.Empty() {
		wc.sendEvent(watch.Event{Type: watch.Modified, Object: newObj})
		metrics.RecordWatchEvent("modified")
		return
	}
	newMatched, _ := wc.pred.Matches(newObj)
	oldObj, oerr := wc.reconstructAt(key, wc.prevTS)
	if oerr != nil {
		klog.V(4).Infof("tikv watcher: cannot reconstruct prior object for %s at ts=%d: %v", key, wc.prevTS, oerr)
	}
	oldMatched := false
	if oldObj != nil {
		oldMatched, _ = wc.pred.Matches(oldObj)
	}
	switch {
	case newMatched && oldMatched:
		wc.sendEvent(watch.Event{Type: watch.Modified, Object: newObj})
		metrics.RecordWatchEvent("modified")
	case newMatched && !oldMatched:
		wc.sendEvent(watch.Event{Type: watch.Added, Object: newObj})
		metrics.RecordWatchEvent("added")
	case !newMatched && oldMatched && oldObj != nil:
		wc.sendEvent(watch.Event{Type: watch.Deleted, Object: oldObj})
		metrics.RecordWatchEvent("deleted")
	}
}

// emitDeleted reconstructs the object as of the previous poll (historical read
// at prevTS, which the compactor's history window keeps readable) and emits a
// Deleted event, honouring the predicate.  If the prior version can no longer
// be read (e.g. GC raced ahead of an unusually long poll gap) the event is
// dropped with a warning; the watch cache reconciles it on its next relist.
func (wc *watchChan) emitDeleted(key []byte) {
	obj, err := wc.reconstructAt(key, wc.prevTS)
	if err != nil || obj == nil {
		klog.Warningf("tikv watcher: cannot reconstruct deleted object for %s at ts=%d: %v", key, wc.prevTS, err)
		return
	}
	if matched, _ := wc.pred.Matches(obj); !matched {
		return
	}
	wc.sendEvent(watch.Event{Type: watch.Deleted, Object: obj})
	metrics.RecordWatchEvent("deleted")
}

// reconstructAt reads a single key at a historical snapshot timestamp and
// decodes it.  Returns (nil, nil) when the key does not exist at ts.
func (wc *watchChan) reconstructAt(key []byte, ts uint64) (runtime.Object, error) {
	snap := wc.watcher.client.GetSnapshot(ts)
	snap.SetNotFillCache(true)
	metrics.RecordWatchReconstructRead(wc.watcher.groupResource)
	val, err := snap.Get(wc.ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	return wc.decode(val, ts)
}

// sendInitialEventsEnd emits the mandatory WatchList InitialEventsEnd bookmark,
// carrying the initial-events-end annotation and the seed's snapshot RV.
func (wc *watchChan) sendInitialEventsEnd(ts uint64) {
	if wc.watcher.newFunc == nil {
		return
	}
	bm := wc.watcher.newFunc()
	if bm == nil {
		return
	}
	if err := wc.watcher.versioner.UpdateObject(bm, ts); err != nil {
		return
	}
	if accessor, accErr := meta.Accessor(bm); accErr == nil {
		anns := accessor.GetAnnotations()
		if anns == nil {
			anns = map[string]string{}
		}
		anns[metav1.InitialEventsAnnotationKey] = "true"
		accessor.SetAnnotations(anns)
	}
	wc.sendEvent(watch.Event{Type: watch.Bookmark, Object: bm})
	metrics.RecordWatchEvent("bookmark")
}

// decode transforms, decodes, and sets the resourceVersion on a stored value.
// The rev embedded in the value's header is used as the object's
// resourceVersion when present; for legacy unwrapped values it falls back to
// the snapshot's timestamp so the watch stream still surfaces a non-zero RV.
func (wc *watchChan) decode(rawValue []byte, revision uint64) (runtime.Object, error) {
	data, _, err := wc.watcher.transformer.TransformFromStorage(
		wc.ctx, rawValue, authenticatedDataString(wc.key))
	if err != nil {
		return nil, err
	}
	objRev, encoded := decodeWithRev(data)
	if objRev == 0 {
		objRev = revision
	}
	obj := wc.watcher.newFunc()
	if _, _, err := wc.watcher.codec.Decode(encoded, nil, obj); err != nil {
		return nil, err
	}
	if err := wc.watcher.versioner.UpdateObject(obj, objRev); err != nil {
		return nil, err
	}
	return obj, nil
}

func (wc *watchChan) sendEvent(e watch.Event) {
	select {
	case wc.resultCh <- e:
	case <-wc.ctx.Done():
	}
}

func (wc *watchChan) handleError(err error) {
	select {
	case wc.errCh <- err:
	default:
	}
	// Send an error event so consumers notice before the channel closes.
	var st metav1.Status
	if apiStatus, ok := err.(apierrors.APIStatus); ok {
		st = apiStatus.Status()
	} else {
		st = apierrors.NewInternalError(err).Status()
	}
	wc.sendEvent(watch.Event{Type: watch.Error, Object: &st})
}
