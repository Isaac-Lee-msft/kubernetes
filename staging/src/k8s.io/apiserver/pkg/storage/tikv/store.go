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

// Package tikv provides a storage.Interface implementation backed by TiKV,
// a Raft-based distributed key-value store (https://github.com/tikv/tikv).
package tikv

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/tikv/metrics"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
)

// authenticatedDataString satisfies value.Context.  It uses the key to
// authenticate the stored data so that values cannot be replayed under a
// different key (same pattern as the etcd3 backend).
type authenticatedDataString []byte

func (a authenticatedDataString) AuthenticatedData() []byte { return []byte(a) }

var _ value.Context = authenticatedDataString(nil)

// Bounded-scan tunables.  A LIST or Count over a large resource must never
// materialise the whole key range in one RPC: the tikv client-go scanner
// fetches the range in batches of this many keys and replaces (frees) the
// previous batch as it advances, so capping the batch size bounds the peak
// transient memory held on both TiKV (one Scan response) and the apiserver
// (one decoded batch) independent of the total resource size.  They are
// overridable via environment so they can be tuned on a running control plane
// without a rebuild.
var (
	// listScanBatchSize bounds the per-RPC key count for LIST scans.
	listScanBatchSize = envInt("KUBE_APISERVER_TIKV_LIST_BATCH_SIZE", 256)
	// countScanBatchSize bounds the per-RPC key count for keys-only Count scans.
	// Count transfers no values so it can use a larger batch.
	countScanBatchSize = envInt("KUBE_APISERVER_TIKV_COUNT_BATCH_SIZE", 1024)
	// maxObjectBytes caps the serialized size of a single stored object,
	// rejecting larger writes the way etcd's --max-request-bytes does.  Unlike
	// etcd, TiKV imposes no object-size or total-keyspace cap, so without this
	// guard a tenant can persist arbitrarily large payloads that later OOM the
	// apiserver on every watch-cache relist (which lists the whole resource).
	// 0 disables the cap.  Default 1.5 MiB matches etcd's effective limit.
	maxObjectBytes = envInt64("KUBE_APISERVER_TIKV_MAX_OBJECT_BYTES", 1572864)
	// listMaxBytes is the fail-safe ceiling on the total serialized bytes an
	// unpaginated (limit==0) LIST may accumulate before it aborts with a
	// retriable error instead of letting the apiserver heap grow until the
	// container is OOM-killed.  A failed LIST is recoverable (the caller can
	// page); an OOM crash loop that blocks deleting the offending data is not.
	// 0 disables the guard.  Default 1 GiB.
	listMaxBytes = envInt64("KUBE_APISERVER_TIKV_LIST_MAX_BYTES", 1<<30)
	// listDecodeConcurrency is the number of worker goroutines used to decode
	// (decrypt + codec-decode) the items of an unpaginated LIST in parallel.
	// Decode/decrypt is CPU-bound and, for a large full-collection relist (the
	// watch-cache seed), dominates the time to produce the list.  Fanning it
	// out across cores cuts that latency roughly linearly.  1 disables the
	// parallel path (pure serial decode).  Default min(GOMAXPROCS, 8).
	listDecodeConcurrency = envInt("KUBE_APISERVER_TIKV_LIST_DECODE_CONCURRENCY", defaultListDecodeConcurrency())
	// listDecodeBatch bounds how many scanned values are buffered and decoded
	// together by the parallel path.  It caps the extra transient memory of
	// that path (≈ listDecodeBatch × object size) and the work handed to the
	// worker pool per round.  Default 64.
	listDecodeBatch = envInt("KUBE_APISERVER_TIKV_LIST_DECODE_BATCH", 64)
)

// defaultListDecodeConcurrency returns the default LIST decode parallelism:
// the process's GOMAXPROCS, capped at 8 so a large list cannot monopolise every
// core (and to bound the buffered-page memory).
func defaultListDecodeConcurrency() int {
	n := goruntime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	if n > 8 {
		n = 8
	}
	return n
}

// envInt reads a positive integer from the named environment variable, falling
// back to def when unset, empty, non-numeric, or non-positive.
func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// envInt64 reads a non-negative int64 from the named environment variable,
// falling back to def when unset, empty, non-numeric, or negative.  Zero is a
// permitted value (used to disable a cap).
func envInt64(name string, def int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// checkObjectSize rejects a write whose serialized object exceeds
// maxObjectBytes, returning a RequestEntityTooLarge (HTTP 413) error so clients
// get a clean rejection instead of silently persisting a payload that later
// OOMs every relist of the resource.
func checkObjectSize(key string, n int) error {
	if maxObjectBytes > 0 && int64(n) > maxObjectBytes {
		return apierrors.NewRequestEntityTooLargeError(
			fmt.Sprintf("object %q is %d bytes which exceeds the %d byte per-object storage limit", key, n, maxObjectBytes))
	}
	return nil
}

// store implements storage.Interface on top of TiKV using the TxnKV API.
type store struct {
	client         *txnkv.Client
	codec          runtime.Codec
	versioner      storage.Versioner
	transformer    value.Transformer
	pathPrefix     string // always ends in "/"
	groupResource  schema.GroupResource
	tikvWatcher    *watcher
	ttlMgr         *ttlManager
	compactor      Compactor
	healthChecker  *healthChecker
	newListFunc    func() runtime.Object
	resourcePrefix string
}

var _ storage.Interface = (*store)(nil)

// New creates a store backed by TiKV.
func New(
	client *txnkv.Client,
	comp Compactor,
	codec runtime.Codec,
	newFunc, newListFunc func() runtime.Object,
	prefix, resourcePrefix string,
	groupResource schema.GroupResource,
	transformer value.Transformer,
	pollInterval time.Duration,
	reaperInterval time.Duration,
) *store {
	// Normalise pathPrefix: always starts and ends with "/".
	pathPrefix := path.Join("/", prefix)
	if !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix += "/"
	}

	objectType := "<unknown>"
	if newFunc != nil {
		objectType = reflect.TypeOf(newFunc()).String()
	}

	w := &watcher{
		client:        client,
		codec:         codec,
		versioner:     APIObjectVersioner,
		transformer:   transformer,
		newFunc:       newFunc,
		objectType:    objectType,
		groupResource: groupResource,
		pollInterval:  pollInterval,
	}

	s := &store{
		client:         client,
		codec:          codec,
		versioner:      APIObjectVersioner,
		transformer:    transformer,
		pathPrefix:     pathPrefix,
		groupResource:  groupResource,
		tikvWatcher:    w,
		ttlMgr:         newTTLManager(client, pathPrefix, reaperInterval),
		compactor:      comp,
		healthChecker:  newHealthChecker(client),
		newListFunc:    newListFunc,
		resourcePrefix: resourcePrefix,
	}
	w.getCurrentStorageRV = func(ctx context.Context) (uint64, error) {
		return s.GetCurrentResourceVersion(ctx)
	}
	return s
}

// prepareKey validates and normalises a raw key to a full TiKV key.
func (s *store) prepareKey(key string) ([]byte, error) {
	if key == "." || key == ".." ||
		strings.Contains(key, "/../") || strings.Contains(key, "/./") ||
		strings.HasPrefix(key, "../") || strings.HasPrefix(key, "./") {
		return nil, fmt.Errorf("invalid storage key %q", key)
	}
	k := strings.TrimLeft(key, "/")
	return []byte(s.pathPrefix + k), nil
}

// --------------------------------------------------------------------------
// storage.Interface implementation
// --------------------------------------------------------------------------

// Versioner implements storage.Interface.
func (s *store) Versioner() storage.Versioner { return s.versioner }

// CompactRevision implements storage.Interface.
func (s *store) CompactRevision() int64 {
	if s.compactor != nil {
		return s.compactor.CompactRevision()
	}
	return 0
}

// ReadinessCheck implements storage.Interface.
func (s *store) ReadinessCheck() error {
	return s.healthChecker.ReadinessCheck()
}

// RequestWatchProgress implements storage.Interface.
// TiKV does not have a server-side watch stream; progress is emitted by the
// polling watcher automatically based on poll ticks.  This is a no-op.
func (s *store) RequestWatchProgress(_ context.Context) error { return nil }

// Close shuts down background goroutines owned by the store (TTL reaper).
// The compactor is stopped separately by the factory's destroyFunc.
func (s *store) Close() {
	s.ttlMgr.Stop()
}

// SetKeysFunc implements storage.Interface.
func (s *store) SetKeysFunc(_ storage.KeysFunc) {}

// GetCurrentResourceVersion implements storage.Interface.
// Returns the latest TSO from PD, which is the monotonically increasing global
// clock that serves as the Kubernetes resourceVersion for the TiKV backend.
func (s *store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	ts, err := s.client.GetTimestamp(ctx)
	if err != nil {
		return 0, fmt.Errorf("tikv: failed to get current timestamp: %w", err)
	}
	return ts, nil
}

// Create implements storage.Interface.
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	defer s.traceSlowOp("Create", key)()
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return storage.ErrResourceVersionSetOnCreate
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("tikv: PrepareObjectForStorage failed: %w", err)
	}

	data, err := runtime.Encode(s.codec, obj)
	if err != nil {
		return err
	}
	if err := checkObjectSize(key, len(data)); err != nil {
		metrics.RecordRequest("create", s.groupResource, err, time.Now())
		return err
	}

	startTime := time.Now()
	var commitTS uint64
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		txn, err := s.client.Begin()
		if err != nil {
			return err
		}

		// Verify the key does not exist (create-only semantics).
		existing, err := txn.Get(ctx, preparedKey)
		if err != nil && !tikverr.IsErrNotFound(err) {
			_ = txn.Rollback()
			return err
		}
		if len(existing) > 0 {
			_ = txn.Rollback()
			metrics.RecordRequest("create", s.groupResource, apierrors.NewAlreadyExists(s.groupResource, key), startTime)
			return storage.NewKeyExistsError(string(preparedKey), 0)
		}

		// Reserve a TSO BEFORE Commit and use it as the commit-time RV.
		// PD's TSO is monotonic, so any timestamp obtained before Commit
		// returns is guaranteed to be <= the actual commit_ts assigned to
		// this transaction by the 2PC protocol.  We embed the same value as
		// the rev header on the stored payload so subsequent reads return
		// an identical, stable resourceVersion -- without which Kubernetes
		// CAS semantics (preconditions, GuaranteedUpdate) cannot work.
		tsReserved, tsErr := s.client.GetTimestamp(ctx)
		if tsErr != nil {
			_ = txn.Rollback()
			return fmt.Errorf("tikv: failed to reserve commit TS: %w", tsErr)
		}
		wrapped := encodeWithRev(tsReserved, data)
		transformed, err := s.transformer.TransformToStorage(ctx, wrapped, authenticatedDataString(preparedKey))
		if err != nil {
			_ = txn.Rollback()
			return storage.NewInternalError(err)
		}

		if err := txn.Set(preparedKey, transformed); err != nil {
			_ = txn.Rollback()
			return err
		}
		if ttl > 0 {
			if err := s.ttlMgr.SetTTL(txn, preparedKey, ttl); err != nil {
				_ = txn.Rollback()
				return err
			}
		}
		if err := txn.Commit(ctx); err != nil {
			if tikverr.IsErrWriteConflict(err) {
				metrics.RecordTxnConflict(s.groupResource)
				if berr := sleepConflictBackoff(ctx, attempt); berr != nil {
					return berr
				}
				continue
			}
			metrics.RecordRequest("create", s.groupResource, err, startTime)
			return err
		}
		commitTS = tsReserved
		break
	}

	metrics.RecordRequest("create", s.groupResource, nil, startTime)

	if out != nil {
		if err := s.versioner.UpdateObject(out, commitTS); err != nil {
			return err
		}
		if _, _, err := s.codec.Decode(data, nil, out); err != nil {
			return err
		}
		if err := s.versioner.UpdateObject(out, commitTS); err != nil {
			return err
		}
	}
	return nil
}

// Delete implements storage.Interface.
func (s *store) Delete(
	ctx context.Context,
	key string,
	out runtime.Object,
	preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc,
	cachedExistingObject runtime.Object,
	opts storage.DeleteOptions,
) error {
	defer s.traceSlowOp("Delete", key)()
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}
	if _, err := conversion.EnforcePtr(out); err != nil {
		return fmt.Errorf("tikv: unable to convert output object to pointer: %w", err)
	}

	var currentState *objState
	if cachedExistingObject != nil {
		currentState, err = s.getStateFromObject(cachedExistingObject)
		if err != nil {
			return err
		}
	} else {
		currentState, err = s.getCurrentState(ctx, preparedKey, out, false)
		if err != nil {
			return err
		}
	}

	for {
		if preconditions != nil {
			if err := preconditions.Check(key, currentState.obj); err != nil {
				// Re-read in case cached state was stale.
				fresh, ferr := s.getCurrentState(ctx, preparedKey, out, false)
				if ferr != nil {
					return ferr
				}
				if fresh.rev == currentState.rev {
					return err // not stale
				}
				currentState = fresh
				continue
			}
		}
		if err := validateDeletion(ctx, currentState.obj); err != nil {
			fresh, ferr := s.getCurrentState(ctx, preparedKey, out, false)
			if ferr != nil {
				return ferr
			}
			if fresh.rev == currentState.rev {
				return err
			}
			currentState = fresh
			continue
		}

		startTime := time.Now()
		txn, err := s.client.Begin()
		if err != nil {
			return err
		}

		// Read at the txn's start TS to get the current revision for CAS.
		existing, err := txn.Get(ctx, preparedKey)
		if tikverr.IsErrNotFound(err) || len(existing) == 0 {
			_ = txn.Rollback()
			metrics.RecordRequest("delete", s.groupResource, nil, startTime)
			return storage.NewKeyNotFoundError(string(preparedKey), 0)
		}
		if err != nil {
			_ = txn.Rollback()
			return err
		}

		if err := txn.Delete(preparedKey); err != nil {
			_ = txn.Rollback()
			return err
		}
		_ = s.ttlMgr.ClearTTL(txn, preparedKey)

		if err := txn.Commit(ctx); err != nil {
			_ = txn.Rollback()
			if tikverr.IsErrWriteConflict(err) {
				metrics.RecordTxnConflict(s.groupResource)
				// Re-read and retry.
				currentState, err = s.getCurrentState(ctx, preparedKey, out, false)
				if err != nil {
					return err
				}
				continue
			}
			metrics.RecordRequest("delete", s.groupResource, err, startTime)
			return err
		}
		metrics.RecordRequest("delete", s.groupResource, nil, startTime)

		if _, _, err := s.codec.Decode(currentState.data, nil, out); err != nil {
			return err
		}
		return nil
	}
}

// Watch implements storage.Interface.
func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	defer s.traceSlowOp("Watch", key)()
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return nil, err
	}
	// Validate against preparedKey (the /registry-qualified prefix), matching
	// GetList and the etcd3 backend.  Watch discards the decoded continue key
	// today, but using the prepared prefix avoids the latent mis-prefixing trap
	// if Watch ever honors a continue token.
	rev, _, err := storage.ValidateListOptions(string(preparedKey), s.versioner, opts)
	if err != nil {
		return nil, err
	}
	return s.tikvWatcher.Watch(ctx, string(preparedKey), rev, opts)
}

// Get implements storage.Interface.
func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	defer s.traceSlowOp("Get", key)()
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	// Resolve the read revision per storage.GetOptions semantics:
	//   ResourceVersion == ""  -> read at current TSO (no consistency guarantee
	//                             stronger than the implicit txn snapshot).
	//   ResourceVersion == "0" -> like "", but explicitly "may serve stale" -- we
	//                             still read current; this is a best-effort guarantee.
	//   ResourceVersion >  0   -> read AT the requested TSO via KVSnapshot.
	// ResourceVersionMatch is honoured only for Exact equality where we must
	// not return any object with a different commit_ts; without per-key
	// commit_ts tracking we can only enforce "read at this TS or fail".
	parsedRV, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}

	startTime := time.Now()

	var (
		val    []byte
		getErr error
	)
	if parsedRV > 0 {
		snap := s.client.GetSnapshot(parsedRV)
		val, getErr = snap.Get(ctx, preparedKey)
	} else {
		txn, txErr := s.client.Begin()
		if txErr != nil {
			return txErr
		}
		defer txn.Rollback() //nolint:errcheck
		val, getErr = txn.Get(ctx, preparedKey)
	}

	metrics.RecordRequest("get", s.groupResource, getErr, startTime)
	if tikverr.IsErrNotFound(getErr) || len(val) == 0 {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return storage.NewKeyNotFoundError(string(preparedKey), int64(parsedRV))
	}
	if getErr != nil {
		return getErr
	}

	data, _, err := s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(preparedKey))
	if err != nil {
		return storage.NewInternalError(err)
	}
	objRev, encoded := decodeWithRev(data)
	if objRev == 0 {
		// Legacy value written without a rev header.  Synthesise a STABLE rev
		// from the payload bytes (NOT the read snapshot's TS, which changes on
		// every read).  This must match the rev that getCurrentState reports
		// for the same bytes, otherwise optimistic-concurrency callers (e.g.
		// the IP/port range allocators) read one RV here and observe a
		// different one inside GuaranteedUpdate and their CAS never converges.
		// The next GuaranteedUpdate on this key rewrites it with a real header.
		objRev = legacyRev(encoded)
	}
	if _, _, err := s.codec.Decode(encoded, nil, out); err != nil {
		return err
	}
	return s.versioner.UpdateObject(out, objRev)
}

// getNewListItemFunc returns a constructor for a fresh instance of the list's
// element type, so decoded items are converted to the list's expected version
// (e.g. v1.Endpoints) rather than the codec's internal version.
func getNewListItemFunc(listObj runtime.Object, v reflect.Value) func() runtime.Object {
	// For unstructured lists with a target group/version, preserve the
	// group/version in the instantiated list items.
	if unstructuredList, isUnstructured := listObj.(*unstructured.UnstructuredList); isUnstructured {
		if apiVersion := unstructuredList.GetAPIVersion(); len(apiVersion) > 0 {
			return func() runtime.Object {
				return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": apiVersion}}
			}
		}
	}

	// Otherwise just instantiate an empty item of the slice's element type.
	elem := v.Type().Elem()
	return func() runtime.Object {
		return reflect.New(elem).Interface().(runtime.Object)
	}
}

// GetList implements storage.Interface.
func (s *store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	defer s.traceSlowOp("GetList", key)()
	listPtr, err := apimeta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return fmt.Errorf("tikv: need a pointer to slice, got %v", v.Kind())
	}
	newItemFunc := getNewListItemFunc(listObj, v)

	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}

	// For a recursive LIST the prepared key is a prefix, so append a trailing
	// "/" if missing (etcd3 parity).  This guards against a sibling resource
	// whose name extends this one (e.g. "/registry/configmaps" matching
	// "/registry/configmapsX") and, critically, fixes the continue-token
	// prefix: the SAME prefix string must be used to (a) decode the continue
	// token, (b) build the scan range, and (c) encode the next token.  Without
	// this normalisation those three diverged and continuation pages were
	// mis-positioned before the whole /registry keyspace.
	if opts.Recursive && !bytes.HasSuffix(preparedKey, []byte("/")) {
		preparedKey = append(cloneBytes(preparedKey), '/')
	}

	// Validate options and extract start key / revision.  The continue token
	// must be decoded against preparedKey (the /registry-qualified prefix that
	// the emit side encodes against), NOT the raw API key: DecodeContinue
	// reattaches this prefix to the stored prefix-relative start key, so a raw
	// key here would drop the storage prefix and make the continuation scan
	// start before the entire /registry keyspace.
	withRev, continueKey, err := storage.ValidateListOptions(string(preparedKey), s.versioner, opts)
	if err != nil {
		return err
	}

	startKey := preparedKey
	if continueKey != "" {
		startKey = []byte(continueKey)
	}

	var endKey []byte
	if opts.Recursive {
		endKey = prefixEnd(preparedKey)
	} else {
		endKey = append(cloneBytes(preparedKey), 0x00)
	}

	startTime := time.Now()

	// Pin a single MVCC snapshot for the whole LIST.  When the caller (or a
	// continue token) requested a specific revision, read at that TSO; for a
	// fresh list with no RV (e.g. the watch-cache relist) reserve the current
	// TSO ONCE so that all pages -- whether fetched internally below or by a
	// follow-up continue request -- observe a single consistent view (R2).
	scanTS := uint64(withRev)
	if scanTS == 0 {
		ts, tsErr := s.client.GetTimestamp(ctx)
		if tsErr != nil {
			metrics.RecordRequest("list", s.groupResource, tsErr, startTime)
			return tsErr
		}
		scanTS = ts
	}

	// Bound the scan so a LIST over a large resource never materialises the
	// whole key range at once.  SetScanBatchSize caps how many keys each Scan
	// RPC fetches; the scanner frees the previous batch as it advances, so the
	// peak transient memory is one batch regardless of total resource size
	// (R3).  SetNotFillCache avoids evicting hot keys from TiKV's block cache
	// with a one-shot bulk scan.
	snap := s.client.GetSnapshot(scanTS)
	snap.SetScanBatchSize(listScanBatchSize)
	snap.SetNotFillCache(true)
	iter, err := snap.Iter(startKey, endKey)
	if err != nil {
		metrics.RecordRequest("list", s.groupResource, err, startTime)
		return err
	}
	defer iter.Close()

	ttlPrefix := s.ttlMgr.ttlScanPrefix()

	// lastIterKey tracks the most recently *visited* key (regardless of
	// whether it matched the predicate).  The next continue token must be
	// derived from this position, not from the last accepted item; otherwise
	// when a page is filled entirely by filtered-out rows the token would
	// stall and we'd loop forever.
	var lastIterKey []byte
	var count int64
	var visited, scannedBytes, accumulatedBytes int64
	limit := int64(opts.Predicate.Limit)

	// For the dominant cost -- an unpaginated (limit==0) full-collection list,
	// i.e. the watch-cache seed and a default `kubectl get` -- the time is
	// spent decrypting and codec-decoding every object.  Fan that CPU-bound
	// work out across a worker pool so a large relist is produced in roughly
	// 1/Nth the time, while preserving scan order and the F3 byte ceiling.
	// Paginated reads (limit>0) are already small and fast, so they stay on the
	// simpler serial path with its exact pagination/continue semantics.
	if limit == 0 && listDecodeConcurrency > 1 {
		count, visited, scannedBytes, lastIterKey, err = s.getListParallel(
			ctx, iter, ttlPrefix, newItemFunc, opts.Predicate, v, key, startTime)
		if err != nil {
			return err
		}
	} else {
		for iter.Valid() {
			if limit > 0 && count >= limit {
				break
			}

			rawKey := cloneBytes(iter.Key())
			rawVal := iter.Value() // batch-owned; consumed before iter.Next()
			lastIterKey = rawKey
			visited++
			scannedBytes += int64(len(rawVal))

			// Skip TTL metadata keys.  Match by HasPrefix on the full TTL scan
			// prefix; bytes.Contains would falsely match real keys that happen
			// to contain the substring "__ttl__/".
			if bytes.HasPrefix(rawKey, ttlPrefix) {
				if err := iter.Next(); err != nil {
					break
				}
				continue
			}

			res := s.decodeListItem(ctx, rawKey, rawVal, newItemFunc, opts.Predicate)
			if res.err != nil {
				metrics.RecordRequest("list", s.groupResource, res.err, startTime)
				return res.err
			}
			if !res.skip && res.matched {
				v.Set(reflect.Append(v, reflect.ValueOf(res.obj).Elem()))
				count++
				accumulatedBytes += int64(res.encoded)

				// Fail-safe (F3): an unpaginated (limit==0) relist accumulates
				// the whole decoded resource in the result slice -- the
				// apiserver-side memory the watch-cache seed grows.  If that
				// crosses the ceiling, abort with a retriable 413 instead of
				// letting the container OOM.  The caller can recover by paging;
				// an OOM crash loop that prevents deleting the offending data
				// cannot.  Paginated callers are bounded by limit already.
				if limit == 0 && listMaxBytes > 0 && accumulatedBytes > listMaxBytes {
					err := listTooLargeError(s.groupResource, listMaxBytes, accumulatedBytes, count)
					metrics.RecordRequest("list", s.groupResource, err, startTime)
					klog.InfoS("tikv GetList aborted: unpaginated list exceeded byte ceiling",
						"resource", s.groupResource.String(), "key", key,
						"accumulatedBytes", accumulatedBytes, "limitBytes", listMaxBytes,
						"items", count)
					return err
				}
			}

			if err := iter.Next(); err != nil {
				break
			}
		}
	}
	metrics.RecordRequest("list", s.groupResource, nil, startTime)
	pages := 0
	if listScanBatchSize > 0 {
		pages = int((visited + int64(listScanBatchSize) - 1) / int64(listScanBatchSize))
	}
	metrics.RecordList(s.groupResource, int(count), scannedBytes, pages)
	// A completed unpaginated scan visited the whole resource, so scannedBytes
	// is the approximate on-storage total -- publish it so keyspace growth that
	// precedes a list OOM is observable/alertable (F1 visibility).
	if limit == 0 {
		metrics.UpdateResourceTotalBytes(s.groupResource, scannedBytes)
	}
	klog.V(4).InfoS("tikv GetList complete",
		"resource", s.groupResource.String(), "key", key,
		"limit", limit, "returned", count, "scannedKeys", visited,
		"scannedBytes", scannedBytes, "pages", pages, "snapshotTS", scanTS,
		"elapsed", time.Since(startTime))

	// If we capped at limit and there are more items, set a continue token
	// derived from the last iterator position so the next page begins after it.
	if limit > 0 && count >= limit && iter.Valid() && lastIterKey != nil {
		continueToken, err := storage.EncodeContinue(string(lastIterKey)+"\x00", string(preparedKey), int64(scanTS))
		if err != nil {
			return err
		}
		return s.versioner.UpdateList(listObj, scanTS, continueToken, nil)
	}
	return s.versioner.UpdateList(listObj, scanTS, "", nil)
}

// listDecodeResult is the outcome of decoding one scanned key/value for GetList.
type listDecodeResult struct {
	obj     runtime.Object // decoded object (valid only when !skip && err == nil)
	encoded int            // serialized object size, for the F3 byte ceiling
	matched bool           // passed the selection predicate
	skip    bool           // soft error (transform/decode/version): drop this item
	err     error          // hard error (predicate evaluation): abort the list
}

// decodeListItem performs the CPU-heavy per-item work of a LIST: transform
// (decrypt), strip the rev header, codec-decode into a fresh element-typed
// object, stamp the resourceVersion, and evaluate the selection predicate.  It
// reads no shared mutable state and writes only its return value, so it is safe
// to run concurrently across the items of a page; the caller appends matched
// objects to the result slice serially in scan order.
func (s *store) decodeListItem(ctx context.Context, rawKey, rawVal []byte, newItem func() runtime.Object, pred storage.SelectionPredicate) listDecodeResult {
	data, _, err := s.transformer.TransformFromStorage(ctx, rawVal, authenticatedDataString(rawKey))
	if err != nil {
		klog.V(4).Infof("tikv GetList: transform error for key %s: %v", rawKey, err)
		return listDecodeResult{skip: true}
	}
	objRev, encoded := decodeWithRev(data)
	if objRev == 0 {
		// Legacy value: synthesise a STABLE, payload-derived rev (matching
		// Get/getCurrentState) rather than the iteration snapshot's TS.
		objRev = legacyRev(encoded)
	}
	// Decode into a freshly allocated instance of the list's element type, NOT
	// a nil object: nil makes the codec return the internal/in-memory type
	// (e.g. core.Endpoints), which cannot be appended into an external-typed
	// slice (e.g. []v1.Endpoints) and panics in reflect.Append.
	obj, _, err := s.codec.Decode(encoded, nil, newItem())
	if err != nil {
		klog.V(4).Infof("tikv GetList: decode error for key %s: %v", rawKey, err)
		return listDecodeResult{skip: true}
	}
	if err := s.versioner.UpdateObject(obj, objRev); err != nil {
		return listDecodeResult{skip: true}
	}
	matched, err := pred.Matches(obj)
	if err != nil {
		return listDecodeResult{err: err}
	}
	return listDecodeResult{obj: obj, encoded: len(encoded), matched: matched}
}

// getListParallel serves an unpaginated (limit==0) LIST by decoding scanned
// items with a bounded worker pool.  The single snapshot iterator is consumed
// serially by this goroutine (TiKV iterators are not concurrency-safe); each
// fixed-size page of raw key/values is then decoded in parallel and the matched
// objects are appended to v in scan order, so the result is byte-identical to
// the serial path while the dominant decrypt/decode cost is spread across
// cores.  Transient memory is bounded by one page (≈ listDecodeBatch × object
// size); the F3 byte ceiling on the accumulated result is still enforced.
func (s *store) getListParallel(
	ctx context.Context,
	iter scanIterator,
	ttlPrefix []byte,
	newItem func() runtime.Object,
	pred storage.SelectionPredicate,
	v reflect.Value,
	key string,
	startTime time.Time,
) (count, visited, scannedBytes int64, lastIterKey []byte, err error) {
	type rawItem struct{ key, val []byte }
	page := make([]rawItem, 0, listDecodeBatch)
	results := make([]listDecodeResult, listDecodeBatch)
	var accumulatedBytes int64

	flush := func() error {
		if len(page) == 0 {
			return nil
		}
		res := results[:len(page)]
		workers := listDecodeConcurrency
		if workers > len(page) {
			workers = len(page)
		}
		if workers <= 1 {
			for i := range page {
				res[i] = s.decodeListItem(ctx, page[i].key, page[i].val, newItem, pred)
			}
		} else {
			var wg sync.WaitGroup
			next := int32(-1)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(atomic.AddInt32(&next, 1))
						if i >= len(page) {
							return
						}
						res[i] = s.decodeListItem(ctx, page[i].key, page[i].val, newItem, pred)
					}
				}()
			}
			wg.Wait()
		}
		// Append matched objects serially, preserving scan order.
		for i := range res {
			r := res[i]
			if r.err != nil {
				metrics.RecordRequest("list", s.groupResource, r.err, startTime)
				return r.err
			}
			if r.skip || !r.matched {
				continue
			}
			v.Set(reflect.Append(v, reflect.ValueOf(r.obj).Elem()))
			count++
			accumulatedBytes += int64(r.encoded)
			if listMaxBytes > 0 && accumulatedBytes > listMaxBytes {
				e := listTooLargeError(s.groupResource, listMaxBytes, accumulatedBytes, count)
				metrics.RecordRequest("list", s.groupResource, e, startTime)
				klog.InfoS("tikv GetList aborted: unpaginated list exceeded byte ceiling",
					"resource", s.groupResource.String(), "key", key,
					"accumulatedBytes", accumulatedBytes, "limitBytes", listMaxBytes,
					"items", count)
				return e
			}
		}
		page = page[:0]
		return nil
	}

	for iter.Valid() {
		rawKey := cloneBytes(iter.Key())
		rawVal := cloneBytes(iter.Value()) // buffered across iter.Next(); must copy
		lastIterKey = rawKey
		visited++
		scannedBytes += int64(len(rawVal))

		if !bytes.HasPrefix(rawKey, ttlPrefix) {
			page = append(page, rawItem{key: rawKey, val: rawVal})
			if len(page) >= listDecodeBatch {
				if ferr := flush(); ferr != nil {
					return count, visited, scannedBytes, lastIterKey, ferr
				}
			}
		}

		if nerr := iter.Next(); nerr != nil {
			break
		}
		if ctx.Err() != nil {
			return count, visited, scannedBytes, lastIterKey, ctx.Err()
		}
	}
	if ferr := flush(); ferr != nil {
		return count, visited, scannedBytes, lastIterKey, ferr
	}
	return count, visited, scannedBytes, lastIterKey, nil
}

// listTooLargeError builds the retriable 413 returned when an unpaginated LIST
// exceeds the configured byte ceiling (F3).
func listTooLargeError(gr schema.GroupResource, limitBytes, accumulated, items int64) error {
	return apierrors.NewRequestEntityTooLargeError(fmt.Sprintf(
		"list of %s exceeds the %d byte server limit (accumulated %d bytes over %d items); retry with pagination (set a limit/continue or kubectl --chunk-size)",
		gr.String(), limitBytes, accumulated, items))
}

// GuaranteedUpdate implements storage.Interface.
func (s *store) GuaranteedUpdate(
	ctx context.Context,
	key string,
	destination runtime.Object,
	ignoreNotFound bool,
	preconditions *storage.Preconditions,
	tryUpdate storage.UpdateFunc,
	cachedExistingObject runtime.Object,
) error {
	defer s.traceSlowOp("GuaranteedUpdate", key)()
	preparedKey, err := s.prepareKey(key)
	if err != nil {
		return err
	}
	if _, err := conversion.EnforcePtr(destination); err != nil {
		return fmt.Errorf("tikv: unable to convert output object to pointer: %w", err)
	}

	var origState *objState
	if cachedExistingObject != nil {
		origState, err = s.getStateFromObject(cachedExistingObject)
		if err != nil {
			return err
		}
	} else {
		origState, err = s.getCurrentState(ctx, preparedKey, destination, ignoreNotFound)
		if err != nil {
			return err
		}
	}

	conflictRetries := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if preconditions != nil {
			if err := preconditions.Check(key, origState.obj); err != nil {
				fresh, ferr := s.getCurrentState(ctx, preparedKey, destination, ignoreNotFound)
				if ferr != nil {
					return ferr
				}
				if fresh.rev == origState.rev {
					return err
				}
				origState = fresh
				continue
			}
		}

		ret, ttl, err := s.updateState(origState, tryUpdate)
		if err != nil {
			fresh, ferr := s.getCurrentState(ctx, preparedKey, destination, ignoreNotFound)
			if ferr != nil {
				return ferr
			}
			if fresh.rev == origState.rev {
				return err
			}
			origState = fresh
			continue
		}

		newData, err := runtime.Encode(s.codec, ret)
		if err != nil {
			return err
		}
		if err := checkObjectSize(key, len(newData)); err != nil {
			return err
		}

		// Short-circuit if the encoded data is identical (no-op write).
		if !origState.stale && bytes.Equal(newData, origState.data) {
			if _, _, err := s.codec.Decode(origState.data, nil, destination); err != nil {
				return err
			}
			return s.versioner.UpdateObject(destination, origState.rev)
		}

		startTime := time.Now()
		// Reserve a TSO BEFORE writing.  PD's TSO is monotonic, so any
		// timestamp obtained before Commit returns is guaranteed to be
		// <= the actual commit_ts.  We embed the reserved value as the rev
		// header on the stored payload AND return it as the new
		// resourceVersion: this gives a stable, per-write rev that is
		// identical to what subsequent reads will report, which is what
		// optimistic-concurrency preconditions require.
		tsReserved, tsErr := s.client.GetTimestamp(ctx)
		if tsErr != nil {
			return fmt.Errorf("tikv: failed to reserve commit TS: %w", tsErr)
		}
		wrapped := encodeWithRev(tsReserved, newData)
		transformedData, err := s.transformer.TransformToStorage(ctx, wrapped, authenticatedDataString(preparedKey))
		if err != nil {
			return storage.NewInternalError(err)
		}

		txn, err := s.client.Begin()
		if err != nil {
			return err
		}

		if err := txn.Set(preparedKey, transformedData); err != nil {
			_ = txn.Rollback()
			return err
		}
		// TTL semantics:
		//   ttl == nil  -> caller does not manage TTL on this update (leave existing)
		//   *ttl  > 0   -> install/refresh TTL
		//   *ttl == 0   -> clear TTL (equivalent to etcd "remove lease")
		if ttl != nil {
			if *ttl > 0 {
				if err := s.ttlMgr.SetTTL(txn, preparedKey, *ttl); err != nil {
					_ = txn.Rollback()
					return err
				}
			} else {
				if err := s.ttlMgr.ClearTTL(txn, preparedKey); err != nil {
					_ = txn.Rollback()
					return err
				}
			}
		}

		commitErr := txn.Commit(ctx)
		metrics.RecordRequest("update", s.groupResource, commitErr, startTime)
		if commitErr != nil {
			if tikverr.IsErrWriteConflict(commitErr) {
				metrics.RecordTxnConflict(s.groupResource)
				metrics.RecordGuaranteedUpdateRetry(s.groupResource)
				klog.V(4).Infof("tikv: GuaranteedUpdate conflict on %s, retrying (attempt %d)", preparedKey, conflictRetries+1)
				if berr := sleepConflictBackoff(ctx, conflictRetries); berr != nil {
					return berr
				}
				conflictRetries++
				origState, err = s.getCurrentState(ctx, preparedKey, destination, ignoreNotFound)
				if err != nil {
					return err
				}
				continue
			}
			return commitErr
		}

		if _, _, err := s.codec.Decode(newData, nil, destination); err != nil {
			return err
		}
		return s.versioner.UpdateObject(destination, tsReserved)
	}
}

// Stats implements storage.Interface.
func (s *store) Stats(ctx context.Context) (storage.Stats, error) {
	startTime := time.Now()

	// Count is keys-only: SetKeyOnly makes TiKV return keys without their
	// values, so counting a large resource transfers no value bytes and stays
	// cheap regardless of object size (R4).  Bound the per-RPC batch and skip
	// the block cache, as for LIST, so a count never balloons memory or evicts
	// hot keys.
	ts, err := s.client.GetTimestamp(ctx)
	if err != nil {
		metrics.RecordRequest("count", s.groupResource, err, startTime)
		return storage.Stats{}, err
	}
	snap := s.client.GetSnapshot(ts)
	snap.SetKeyOnly(true)
	snap.SetNotFillCache(true)
	snap.SetScanBatchSize(countScanBatchSize)

	// Scope the count to this store's resource prefix (matching the etcd3
	// backend), not the global storage prefix -- otherwise every resource's
	// Count would return the cluster-wide object total.  prepareKey prepends
	// the storage path prefix; a trailing "/" restricts the range to children
	// of the resource directory.
	prefixKey, err := s.prepareKey(s.resourcePrefix)
	if err != nil {
		metrics.RecordRequest("count", s.groupResource, err, startTime)
		return storage.Stats{}, err
	}
	if len(prefixKey) == 0 || prefixKey[len(prefixKey)-1] != '/' {
		prefixKey = append(prefixKey, '/')
	}
	startKey := prefixKey
	endKey := prefixEnd(startKey)
	iter, err := snap.Iter(startKey, endKey)
	if err != nil {
		metrics.RecordRequest("count", s.groupResource, err, startTime)
		return storage.Stats{}, err
	}
	defer iter.Close()

	ttlPrefix := s.ttlMgr.ttlScanPrefix()
	var count int64
	for iter.Valid() {
		if !bytes.HasPrefix(iter.Key(), ttlPrefix) {
			count++
		}
		if err := iter.Next(); err != nil {
			break
		}
	}
	metrics.RecordRequest("count", s.groupResource, nil, startTime)
	klog.V(4).InfoS("tikv Stats complete",
		"resource", s.groupResource.String(), "objectCount", count,
		"elapsed", time.Since(startTime))

	// Match the etcd3 backend, which reports only ObjectCount; the average
	// object size is intentionally left unset because computing it would
	// require transferring values, which is exactly what R4 avoids.
	return storage.Stats{ObjectCount: count}, nil
}

// --------------------------------------------------------------------------
// Internal helpers
// --------------------------------------------------------------------------

// slowOpThreshold is the duration above which a storage operation is logged so
// that operators can pinpoint which key/operation is stalling (for example, a
// LIST or Get that cannot complete because the underlying region has no stable
// leader, or a call made with an uncancellable context.TODO() that blocks
// indefinitely).  The persistent "peer is not leader" condition is a
// cluster-side (TiKV/PD) problem rather than a missing retry in this backend --
// reads here are already region-aware and bounded -- but a blocked
// LIST/Get/Create silently prevents informer caches from syncing and stalls
// PostStartHooks.  Emitting log lines for slow ops makes that failure
// observable instead of silent.
const slowOpThreshold = 1 * time.Second

// slowOpReportInterval is how often a still-running operation is re-logged while
// it remains stuck, so an operation that never returns is still visible.
const slowOpReportInterval = 5 * time.Second

// traceSlowOp instruments a public store method.  It starts a watchdog
// goroutine that logs if the operation exceeds slowOpThreshold and keeps
// logging every slowOpReportInterval while it is still running, then logs a
// final line when it completes.  Crucially, because the watchdog fires on a
// timer rather than only when the operation returns, an operation that blocks
// forever (e.g. a read issued with an uncancellable context) is still reported
// by name instead of hanging silently.  Intended to be used with defer:
//
//	defer s.traceSlowOp("Get", key)()
func (s *store) traceSlowOp(op, key string) func() {
	start := time.Now()
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(slowOpThreshold)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-timer.C:
				klog.InfoS("tikv storage op still running",
					"op", op, "resource", s.groupResource.String(), "key", key,
					"elapsed", time.Since(start))
				timer.Reset(slowOpReportInterval)
			}
		}
	}()
	return func() {
		close(done)
		if d := time.Since(start); d > slowOpThreshold {
			klog.InfoS("tikv slow storage op",
				"op", op, "resource", s.groupResource.String(), "key", key,
				"duration", d)
		}
	}
}

const maxConflictBackoff = 1 * time.Second

// sleepConflictBackoff waits before retrying a transaction that failed with a
// TiKV write conflict.  The delay grows exponentially with the retry count and
// is jittered to spread out competing writers; it returns ctx.Err() if the
// context is cancelled while waiting so callers stop retrying promptly.
//
// Optimistic-transaction conflicts are retried until the request succeeds or
// its context is done -- matching the etcd3 backend, which retries optimistic
// updates indefinitely.  A fixed retry budget would surface spurious
// "exceeded max retries" errors on hot keys such as frequently-renewed leases.
func sleepConflictBackoff(ctx context.Context, retries int) error {
	d := 5 * time.Millisecond << uint(retries)
	if d <= 0 || d > maxConflictBackoff {
		d = maxConflictBackoff
	}
	// Apply +/-50% jitter around d/2 .. d.
	d = d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// objState holds the decoded current state of an object.
type objState struct {
	obj   runtime.Object
	rev   uint64
	data  []byte
	stale bool
}

func (s *store) getCurrentState(ctx context.Context, preparedKey []byte, destObj runtime.Object, ignoreNotFound bool) (*objState, error) {
	txn, err := s.client.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback() //nolint:errcheck

	val, err := txn.Get(ctx, preparedKey)
	if tikverr.IsErrNotFound(err) || len(val) == 0 {
		if ignoreNotFound {
			if destObj != nil {
				if zerr := runtime.SetZeroValue(destObj); zerr != nil {
					return nil, zerr
				}
			}
			// A missing key MUST report a stable rev of 0 (the same value
			// etcd3 uses).  GuaranteedUpdate detects "the object did not
			// change between retries" by comparing origState.rev to a fresh
			// re-read; if we synthesised txn.StartTS() here every re-read
			// would yield a new, larger TSO, so that comparison could never
			// hold for a non-existent key.  A caller whose tryUpdate returns
			// an error for the not-yet-created object (e.g. the IP/port
			// allocators' "cannot allocate resources at this time") would
			// then loop forever instead of surfacing the error.
			return &objState{obj: destObj, rev: 0, data: nil}, nil
		}
		return nil, storage.NewKeyNotFoundError(string(preparedKey), 0)
	}
	if err != nil {
		return nil, err
	}

	data, _, err := s.transformer.TransformFromStorage(ctx, val, authenticatedDataString(preparedKey))
	if err != nil {
		return nil, storage.NewInternalError(err)
	}
	objRev, encoded := decodeWithRev(data)
	if objRev == 0 {
		// Legacy unwrapped value written by a pre-header build.  Without a
		// stored commit_ts we synthesise a STABLE rev from the payload bytes
		// rather than the txn's start_ts.  start_ts changes on every read, so
		// using it would make optimistic-concurrency CAS (e.g. the IP/port
		// range allocators' CreateOrUpdate) never observe a matching rev
		// between its Get and its update read -- GuaranteedUpdate would then
		// spin in its conflict-retry loop forever.  A payload-derived rev is
		// identical across repeated reads of unchanged data, so CAS converges;
		// the first GuaranteedUpdate rewrites the value with a real header.
		objRev = legacyRev(encoded)
	}
	// Decode into the caller-provided destination object so the codec
	// converts the stored payload to the destination's (external) version.
	// Decoding into a nil object instead would yield the internal/in-memory
	// type (e.g. core.Endpoints), which GuaranteedUpdate callers' tryUpdate
	// funcs cannot type-assert to their external type (e.g. *v1.Endpoints)
	// and would panic.
	obj, _, err := s.codec.Decode(encoded, nil, destObj)
	if err != nil {
		return nil, err
	}
	if err := s.versioner.UpdateObject(obj, objRev); err != nil {
		return nil, err
	}
	return &objState{obj: obj, rev: objRev, data: encoded}, nil
}

func (s *store) getStateFromObject(obj runtime.Object) (*objState, error) {
	data, err := runtime.Encode(s.codec, obj)
	if err != nil {
		return nil, err
	}
	rv, err := s.versioner.ObjectResourceVersion(obj)
	if err != nil {
		return nil, err
	}
	return &objState{obj: obj, rev: rv, data: data}, nil
}

func (s *store) updateState(st *objState, tryUpdate storage.UpdateFunc) (runtime.Object, *uint64, error) {
	var responseMeta storage.ResponseMeta
	if st.rev != 0 {
		responseMeta.ResourceVersion = st.rev
	}
	ret, ttlPtr, err := tryUpdate(st.obj, responseMeta)
	if err != nil {
		return nil, nil, err
	}
	return ret, ttlPtr, nil
}

// cloneBytes returns a copy of b.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// prefixEnd returns the key that sorts immediately after all keys with the
// given prefix.  It increments the last byte; if that would overflow it strips
// the last byte and recurses.
func prefixEnd(prefix []byte) []byte {
	end := cloneBytes(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
		end = end[:i]
	}
	// All bytes were 0xff; no upper bound.
	return nil
}
