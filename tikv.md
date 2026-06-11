# TiKV Backend Storage Implementation for kube-apiserver

**Status:** Proposal  
**Authors:** *TBD*  
**Created:** 2026-02-27  

---

## Table of Contents

1. [Summary](#summary)
2. [Motivation](#motivation)
3. [Background](#background)
4. [Design Overview](#design-overview)
5. [Detailed Design](#detailed-design)
   - [New Storage Type Constant](#new-storage-type-constant)
   - [Factory Integration](#factory-integration)
   - [TiKV Store Implementation](#tikv-store-implementation)
   - [Key Encoding](#key-encoding)
   - [Watch Implementation](#watch-implementation)
   - [TTL / Lease Emulation](#ttl--lease-emulation)
   - [Transactions and Optimistic Concurrency](#transactions-and-optimistic-concurrency)
   - [Compaction and GC](#compaction-and-gc)
   - [Health and Readiness Checks](#health-and-readiness-checks)
   - [Metrics and Observability](#metrics-and-observability)
6. [Configuration](#configuration)
7. [Migration Path](#migration-path)
8. [Testing Strategy](#testing-strategy)
9. [Performance Considerations](#performance-considerations)
10. [Risks and Mitigations](#risks-and-mitigations)
11. [Alternatives Considered](#alternatives-considered)
12. [Implementation Phases](#implementation-phases)

---

## Summary

This document proposes adding [TiKV](https://github.com/tikv/tikv) as an alternative backend storage engine for the Kubernetes API server (`kube-apiserver`). TiKV is an open-source, distributed, transactional key-value store built in Rust that uses the Raft consensus protocol. It is a graduated CNCF project.

The implementation introduces a new `storage.Interface` backend under `staging/src/k8s.io/apiserver/pkg/storage/tikv/` that can be selected via `--storage-backend=tikv` on the kube-apiserver command line.

## Motivation

| Concern | etcd (current) | TiKV |
|---|---|---|
| **Max recommended data size** | ~8 GB (practical limit with default 2 GB value size limit) | Scales horizontally to multi-TB |
| **Horizontal scalability** | Static cluster membership; scaling requires manual reconfiguration | Auto-sharding via PD (Placement Driver); add nodes to scale |
| **Write throughput** | Single-leader for each key range (whole keyspace) | Multi-Raft groups; concurrent writes across regions |
| **Value size limit** | 1.5 MiB default (configurable up to ~10 MiB) | 6 MiB default per value (configurable); large CRDs are less constrained |
| **Ecosystem** | Kubernetes-specific; limited general adoption | Backing store for TiDB; broad CNCF ecosystem adoption |
| **Operational overhead** | Requires careful compaction, defrag, and snapshot tuning | Automatic region splitting, merging, and balancing via PD |

Goals:
- Provide a production-quality `storage.Interface` implementation backed by TiKV.
- Support all existing Kubernetes storage semantics: MVCC, watch, TTL, pagination, optimistic concurrency.
- Require zero changes to any code above the `storage.Interface` boundary (controllers, REST handlers, watch cache, etc.).
- Allow live migration from etcd to TiKV with minimal downtime.
- Replace etcd as the default storage backend once TiKV has been proven stable through alpha and beta graduation.

Non-Goals:
- Supporting TiKV's transaction mode (SI/SSI isolation); we operate at the raw KV + MVCC level to align with etcd semantics.
- Modifying the watch cache layer (`pkg/storage/cacher/`).

## Background

### Kubernetes Storage Architecture

The kube-apiserver persists all resource state through a narrow `storage.Interface` defined in `staging/src/k8s.io/apiserver/pkg/storage/interfaces.go`. The key methods are:

```go
type Interface interface {
    Versioner() Versioner
    Create(ctx, key, obj, out, ttl) error
    Delete(ctx, key, out, preconditions, validateDeletion, cached, opts) error
    Watch(ctx, key, opts) (watch.Interface, error)
    Get(ctx, key, opts, objPtr) error
    GetList(ctx, key, opts, listObj) error
    GuaranteedUpdate(ctx, key, dest, ignoreNotFound, preconditions, tryUpdate, cached) error
    Stats(ctx) (Stats, error)
    ReadinessCheck() error
    RequestWatchProgress(ctx) error
    GetCurrentResourceVersion(ctx) (uint64, error)
    SetKeysFunc(KeysFunc)
    CompactRevision() int64
}
```

A `storagebackend.Config` struct carries connection and encoding parameters. The factory in `storagebackend/factory/factory.go` switches on `Config.Type` to instantiate the appropriate backend.

Currently only `"etcd3"` (and the default `""` which maps to etcd3) is supported. etcd2 is rejected with an error.

### TiKV Overview

TiKV is a distributed key-value store that:

- Uses **Multi-Raft** for consensus — the keyspace is split into regions (~96 MiB each), each governed by an independent Raft group.
- Provides **MVCC** built on RocksDB — every write is assigned a monotonically increasing timestamp, and old versions are retained until GC.
- Employs a **Placement Driver (PD)** for cluster metadata, timestamp allocation (TSO), and region scheduling.
- Exposes two API layers:
  - **TxnKV** — full distributed transactions with Snapshot Isolation.
  - **RawKV** — simple get/put/delete/scan without transactions.
- Has an official **Go client**: [`github.com/tikv/client-go/v2`](https://github.com/tikv/client-go).

For this proposal we use the **TxnKV** API, since it provides the MVCC timestamps needed to implement Kubernetes `resourceVersion` semantics and compare-and-swap (CAS) operations needed for optimistic concurrency.

---

## Design Overview

```
┌─────────────────────────────────────┐
│          kube-apiserver             │
│  (REST handlers, registry, etc.)   │
└──────────────┬──────────────────────┘
               │  storage.Interface
┌──────────────▼──────────────────────┐
│       pkg/storage/cacher/           │
│       (watch cache - unchanged)     │
└──────────────┬──────────────────────┘
               │  storage.Interface
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌─────────────┐
│  etcd3/     │  │  tikv/      │   ← NEW
│  store.go   │  │  store.go   │
│  watcher.go │  │  watcher.go │
│  lease_mgr  │  │  ttl_mgr.go │
└──────┬──────┘  └──────┬──────┘
       │                │
   etcd gRPC       tikv client-go
       │                │
   ┌───▼───┐      ┌────▼────┐
   │ etcd  │      │  TiKV   │
   │cluster│      │ cluster │
   │       │      │  + PD   │
   └───────┘      └─────────┘
```

---

## Detailed Design

### New Storage Type Constant

In `staging/src/k8s.io/apiserver/pkg/storage/storagebackend/config.go`:

```go
const (
    StorageTypeUnset = ""
    StorageTypeETCD2 = "etcd2"
    StorageTypeETCD3 = "etcd3"
    StorageTypeTiKV  = "tikv"   // NEW
)
```

### Factory Integration

In `staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/factory.go`, extend the `Create`, `CreateHealthCheck`, and `CreateReadyCheck` switches:

```go
func Create(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object,
    resourcePrefix string) (storage.Interface, DestroyFunc, error) {
    switch c.Type {
    case storagebackend.StorageTypeETCD2:
        return nil, nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
    case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
        return newETCD3Storage(c, newFunc, newListFunc, resourcePrefix)
    case storagebackend.StorageTypeTiKV:
        return newTiKVStorage(c, newFunc, newListFunc, resourcePrefix)
    default:
        return nil, nil, fmt.Errorf("unknown storage type: %s", c.Type)
    }
}
```

A new file `storagebackend/factory/tikv.go` implements `newTiKVStorage()`.

### TiKV Store Implementation

New package: `staging/src/k8s.io/apiserver/pkg/storage/tikv/`

```
tikv/
├── store.go          # storage.Interface implementation
├── watcher.go        # watch.Interface implementation + CDC-based event stream
├── ttl_manager.go    # TTL expiration via background goroutine
├── versioner.go      # Maps TiKV MVCC timestamps → Kubernetes resourceVersion
├── compact.go        # GC safepoint management
├── healthcheck.go    # PD + TiKV store connectivity checks
├── metrics/
│   └── metrics.go    # Prometheus metrics mirroring etcd3/metrics patterns
├── store_test.go
├── watcher_test.go
└── integration/
    └── store_test.go # Integration tests against a real TiKV cluster
```

#### Core Struct

```go
package tikv

import (
    "context"
    "sync"

    tikvClient "github.com/tikv/client-go/v2/tikv"
    "github.com/tikv/client-go/v2/txnkv"
    pd "github.com/tikv/pd/client"

    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/apiserver/pkg/storage"
    "k8s.io/apiserver/pkg/storage/value"
)

// store implements storage.Interface backed by TiKV.
type store struct {
    client          *txnkv.Client
    pdClient        pd.Client
    codec           runtime.Codec
    versioner       storage.Versioner
    transformer     value.Transformer
    pathPrefix      string
    groupResource   schema.GroupResource
    watcher         *watcher
    ttlManager      *ttlManager
    decoder         decoder
    newListFunc     func() runtime.Object
    resourcePrefix  string
    compactRevision int64
    mu              sync.RWMutex
}

var _ storage.Interface = (*store)(nil)
```

### Key Encoding

TiKV keys are raw byte slices. We use the same hierarchical `/`-delimited scheme as etcd3:

```
<pathPrefix><resource_key>
```

Where `pathPrefix` defaults to `/registry/` and `resource_key` follows the standard Kubernetes convention:

| Resource | Key Pattern |
|---|---|
| Namespaced | `/<resource>/<namespace>/<name>` |
| Cluster-scoped | `/<resource>/<name>` |

Examples:
- `/registry/pods/default/nginx` 
- `/registry/nodes/worker-1`
- `/registry/configmaps/kube-system/coredns`

For prefix scans (list operations), we compute the key range `[prefix, prefixEnd)` where `prefixEnd` is the lexicographic successor of the prefix (increment the last byte, or append `\x00`). This maps directly to TiKV's `Scan(startKey, endKey, limit)`.

### Watch Implementation

#### Challenge

etcd provides a native server-side Watch API that streams ordered change events from a given revision. TiKV does not have an equivalent built-in watch primitive in its core KV API.

#### Approach: TiKV Change Data Capture (CDC)

[TiCDC](https://github.com/pingcap/tiflow) is the change data capture component for TiKV. However, depending on TiCDC introduces an additional component. We propose a **dual-strategy** approach:

**Strategy A — Polling with MVCC Scan (Initial / Fallback)**

Use TiKV's MVCC capabilities to implement a polling-based watcher:

1. Record the starting timestamp `ts_start` from PD's TSO.
2. Periodically (e.g., every 250ms, configurable) perform an MVCC scan over the watched key range at the latest timestamp.
3. Compare with the previously seen state to compute a diff (created, modified, deleted keys).
4. Emit corresponding `watch.Event` objects.

This is simple but has higher latency and resource cost at scale.

**Strategy B — TiKV CDC Client (Preferred for Production)**

Use the `kv-client` CDC interface exposed by TiKV stores directly:

1. Open a `ChangeDataRequest` gRPC stream to each TiKV store that hosts regions overlapping the watched key range.
2. Receive `CdcEvent` messages (prewrite, commit, rollback) and assemble resolved timestamps.
3. Convert committed mutations into `watch.Event` objects, ordered by commit timestamp.
4. Track resolved timestamps per region and emit bookmark events.

```go
type watcher struct {
    client        *txnkv.Client
    pdClient      pd.Client
    codec         runtime.Codec
    versioner     storage.Versioner
    transformer   value.Transformer
    groupResource schema.GroupResource
    newFunc       func() runtime.Object
}

type watchChan struct {
    watcher        *watcher
    key            string
    keyEnd         string
    initialRev     uint64
    recursive      bool
    progressNotify bool
    pred           storage.SelectionPredicate
    ctx            context.Context
    cancel         context.CancelFunc
    incomingCh     chan *event   // buffered, 100
    resultCh       chan watch.Event // buffered, 100
    errCh          chan error
}
```

**Event Translation:**

| TiKV CDC Event | Kubernetes watch.EventType |
|---|---|
| Put (key not previously existing) | `watch.Added` |
| Put (key previously existing) | `watch.Modified` |
| Delete | `watch.Deleted` |
| ResolvedTs | `watch.Bookmark` (if `progressNotify` set) |

**Initial Events (`SendInitialEvents`):**

When `SendInitialEvents` is requested, the watcher performs an MVCC snapshot read (point-in-time scan) at the requested revision, emits synthetic `Added` events for all matching keys, then transitions to the CDC stream starting from that revision.

### TTL / Lease Emulation

etcd supports leases natively — objects attached to a lease are auto-deleted when the lease expires. TiKV has no lease primitive.

**Design:**

```go
type ttlManager struct {
    client   *txnkv.Client
    mu       sync.Mutex
    stopCh   chan struct{}
}

// ttlKey stores TTL metadata alongside the object.
// Schema: /registry/__ttl/<original_key> → expiration_unix_timestamp
const ttlPrefix = "__ttl/"
```

1. When `Create()` is called with `ttl > 0`, write (atomically, in the same transaction):
   - The object at its normal key.
   - A TTL marker at `<pathPrefix>__ttl/<key>` containing the absolute expiration time (Unix seconds).

2. A background goroutine in `ttlManager` periodically scans the `__ttl/` prefix range:
   - For each marker whose expiration time has passed, delete both the marker and the original key in a transaction.
   - Use compare-and-swap to avoid deleting objects that were updated with a new TTL.

3. Scan interval: configurable, default 10 seconds. The resolution of TTL expiration is thus ±10s, comparable to etcd's lease renewal interval.

4. In multi-apiserver deployments, leader election (or distributed locking via TiKV transactions) ensures only one apiserver runs the TTL reaper.

### Transactions and Optimistic Concurrency

Kubernetes relies on `resourceVersion` for optimistic concurrency control. In etcd, `resourceVersion` maps to the etcd MVCC revision (a cluster-global, monotonically increasing counter). In TiKV, the equivalent is the **TSO timestamp** from PD.

#### Versioner Mapping

```go
type tikvVersioner struct{}

func (v *tikvVersioner) UpdateObject(obj runtime.Object, resourceVersion uint64) error {
    // Set metadata.resourceVersion = strconv.FormatUint(resourceVersion, 10)
}

func (v *tikvVersioner) ParseResourceVersion(resourceVersion string) (uint64, error) {
    return strconv.ParseUint(resourceVersion, 10, 64)
}
```

The `resourceVersion` exposed to Kubernetes clients is the **TiKV commit timestamp** of the transaction that last wrote the object.

#### Create

```go
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
    preparedKey := s.prepareKey(key)
    data, err := runtime.Encode(s.codec, obj)
    // ...
    data, err = s.transformer.TransformToStorage(ctx, data, dataCtx)
    // ...

    txn, err := s.client.Begin()
    // Check key does not exist
    existing, err := txn.Get(ctx, preparedKey)
    if existing != nil {
        return storage.NewKeyExistsError(key, 0)
    }
    txn.Set(preparedKey, data)
    if ttl > 0 {
        s.ttlManager.setTTL(txn, preparedKey, ttl)
    }
    err = txn.Commit(ctx)
    // Set out's resourceVersion to txn.CommitTS()
    s.versioner.UpdateObject(out, txn.CommitTS())
    return nil
}
```

#### GuaranteedUpdate

```go
func (s *store) GuaranteedUpdate(ctx context.Context, key string, dest runtime.Object,
    ignoreNotFound bool, preconditions *storage.Preconditions,
    tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {

    preparedKey := s.prepareKey(key)
    for {
        txn, _ := s.client.Begin()
        existing, err := txn.Get(ctx, preparedKey)
        // Decode existing → currentObj, check preconditions
        // Call tryUpdate(currentObj, ResponseMeta{TTL: ..., ResourceVersion: ...})
        // Encode updated → data
        // TransformToStorage
        txn.Set(preparedKey, data)
        err = txn.Commit(ctx)
        if isWriteConflict(err) {
            continue // retry
        }
        // Set dest's resourceVersion to txn.CommitTS()
        return err
    }
}
```

TiKV's optimistic transaction model naturally retries on write conflicts, which aligns perfectly with `GuaranteedUpdate`'s retry loop.

#### Delete

```go
func (s *store) Delete(ctx context.Context, key string, out runtime.Object,
    preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc,
    cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {

    preparedKey := s.prepareKey(key)
    for {
        txn, _ := s.client.Begin()
        existing, err := txn.Get(ctx, preparedKey)
        if existing == nil {
            return storage.NewKeyNotFoundError(key, 0)
        }
        // Decode, check preconditions, validate
        txn.Delete(preparedKey)
        s.ttlManager.clearTTL(txn, preparedKey)
        err = txn.Commit(ctx)
        if isWriteConflict(err) {
            continue
        }
        return err
    }
}
```

### Compaction and GC

etcd requires periodic compaction to reclaim old revisions. TiKV uses a **GC (Garbage Collection)** mechanism driven by a GC safepoint managed through PD.

- The `tikv/compact.go` module periodically advances the GC safepoint via `pdClient.UpdateServiceGCSafePoint()`.
- The safepoint is set to `now - EventsHistoryWindow` (default 75 seconds), ensuring that MVCC versions needed by active watches are retained.
- TiKV's built-in GC worker handles the actual cleanup of old MVCC versions below the safepoint.
- `CompactRevision()` returns the current GC safepoint timestamp.

```go
type compactor struct {
    pdClient            pd.Client
    serviceID           string
    eventsHistoryWindow time.Duration
    interval            time.Duration
    stopCh              chan struct{}
    lastSafepoint       uint64
    mu                  sync.RWMutex
}

func (c *compactor) run() {
    ticker := time.NewTicker(c.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            safepoint := uint64(time.Now().Add(-c.eventsHistoryWindow).UnixNano())
            // Convert to TiKV TSO format
            c.pdClient.UpdateServiceGCSafePoint(ctx, c.serviceID, 0, safepoint)
            c.mu.Lock()
            c.lastSafepoint = safepoint
            c.mu.Unlock()
        case <-c.stopCh:
            return
        }
    }
}
```

### Health and Readiness Checks

```go
func (s *store) ReadinessCheck() error {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    // 1. Check PD connectivity (cluster metadata)
    _, err := s.pdClient.GetClusterID(ctx)
    if err != nil {
        return fmt.Errorf("tikv PD not reachable: %w", err)
    }

    // 2. Perform a lightweight read to verify TiKV store availability
    txn, err := s.client.Begin()
    if err != nil {
        return fmt.Errorf("tikv transaction begin failed: %w", err)
    }
    _, _ = txn.Get(ctx, []byte("/healthz"))
    txn.Rollback()
    return nil
}
```

Separate health check for the storage factory:

```go
func newTiKVHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
    // Create PD client, connect to PD endpoints from c.Transport.ServerList
    // Return closure that checks PD + TiKV store health
}
```

### Metrics and Observability

Mirror the metrics exposed by `etcd3/metrics/` under a `tikv_` prefix:

| Metric | Description |
|---|---|
| `tikv_request_duration_seconds` | Histogram of TiKV request latency by operation (get, put, delete, scan, txn) |
| `tikv_request_total` | Counter of TiKV requests by operation and status |
| `tikv_object_counts` | Gauge of objects per resource type |
| `tikv_db_total_size_bytes` | Gauge of total storage consumed (from PD store stats) |
| `tikv_watch_events_total` | Counter of watch events emitted by type (added, modified, deleted, bookmark) |
| `tikv_txn_conflict_total` | Counter of transaction conflicts (optimistic lock retries) |
| `tikv_gc_safepoint_timestamp` | Gauge of the current GC safepoint |
| `tikv_watch_open_total` | Gauge of currently active watch streams |

---

## Configuration

### Command-Line Flags

The existing `--storage-backend` flag is extended to accept `"tikv"`:

```
--storage-backend=tikv
```

The `--etcd-servers` flag is reused to specify **PD endpoints** (PD is the entry point for all TiKV cluster operations):

```
--etcd-servers=http://pd1:2379,http://pd2:2379,http://pd3:2379
```

> Note: PD uses the same default port (2379) as etcd, making migrations straightforward.

Additional TiKV-specific flags (optional):

| Flag | Default | Description |
|---|---|---|
| `--tikv-gc-interval` | `5m` | How often to advance the GC safepoint |
| `--tikv-ttl-reap-interval` | `10s` | How often to scan and expire TTL-bound objects |
| `--tikv-watch-poll-interval` | `250ms` | Polling interval for the MVCC-scan watch fallback |
| `--tikv-max-txn-retries` | `5` | Maximum retries for conflicting transactions |

### TLS Configuration

TiKV/PD support mutual TLS. The existing `--etcd-cafile`, `--etcd-certfile`, and `--etcd-keyfile` flags are reused, as PD accepts the same TLS configuration format.

---

## Migration Path

### Phase 1: Dual-Write (Zero Downtime)

1. Deploy TiKV cluster alongside existing etcd.
2. Run a migration controller that:
   - Lists all keys from etcd.
   - Writes each key/value to TiKV, preserving the encoded data.
   - Establishes a watch on etcd to replicate ongoing changes to TiKV.
3. Validate data consistency with a comparison tool.

### Phase 2: Switchover

1. Stop all kube-apiservers.
2. Run a final consistency check.
3. Update kube-apiserver configuration: `--storage-backend=tikv --etcd-servers=<PD endpoints>`.
4. Start kube-apiservers.

### Phase 3: Cleanup

1. Decommission the etcd-to-TiKV replication controller.
2. Decommission the etcd cluster.

### ResourceVersion Continuity

During migration, the `resourceVersion` numbering will reset (TiKV TSO timestamps are independent of etcd revisions). This is acceptable because:
- The watch cache handles reconnection transparently.
- Clients that cache `resourceVersion` will receive `410 Gone` and re-list, which is the standard recovery path.
- List pagination continue tokens are invalidated, but clients retry automatically.

---

## Testing Strategy

### Unit Tests

- All `storage.Interface` methods with a mock TiKV client.
- Key encoding/decoding.
- TTL manager behavior.
- Watch event translation and ordering.
- Transaction conflict retry logic.

### Integration Tests

Reuse the existing storage integration test suite in `staging/src/k8s.io/apiserver/pkg/storage/tests/`:

```go
func TestTiKVCreate(t *testing.T)            { RunTestCreate(t, tikvStore) }
func TestTiKVGet(t *testing.T)               { RunTestGet(t, tikvStore) }
func TestTiKVList(t *testing.T)              { RunTestList(t, tikvStore) }
func TestTiKVGuaranteedUpdate(t *testing.T)  { RunTestGuaranteedUpdate(t, tikvStore) }
func TestTiKVDelete(t *testing.T)            { RunTestDelete(t, tikvStore) }
func TestTiKVWatch(t *testing.T)             { RunTestWatch(t, tikvStore) }
func TestTiKVWatchFromZero(t *testing.T)     { RunTestWatchFromZero(t, tikvStore) }
func TestTiKVPagination(t *testing.T)        { RunTestPagination(t, tikvStore) }
```

These tests run against a real TiKV cluster (spun up via `docker-compose` or `tiup playground` in CI).

### E2E Tests

- Full Kubernetes cluster with `--storage-backend=tikv`.
- Run the standard conformance test suite.
- Run storage-specific e2e tests (watch reliability, pagination under load, TTL expiration).

### Stress / Scale Tests

- 5000-node simulated cluster worth of objects stored in TiKV.
- Concurrent watch streams (1000+).
- Write throughput benchmarks compared to etcd baseline.

---

## Performance Considerations

### Latency

- **Single-key reads** (Get): TiKV point reads go through one network hop to the region leader. Expected latency is comparable to etcd (~1-3ms) in a co-located deployment.
- **Scans** (List): TiKV scans may touch multiple regions. The client-go library handles transparent multi-region scanning. For prefix scans within a single region, latency is comparable. Cross-region scans add ~1ms per additional region.
- **Writes**: Optimistic transactions require 2PC (prewrite + commit). Each phase requires a Raft consensus round. Single-key writes in a single region are ~2-5ms. Multi-key transactions within the same region are similar.

### Throughput

- TiKV's multi-raft architecture distributes writes across regions. For Kubernetes workloads where writes are spread across many resources (and thus many key prefixes), TiKV can achieve significantly higher aggregate write throughput than etcd.
- Resource types that are heavily written (Events, Endpoints, Leases) naturally land in separate regions.

### Watch Latency

- The CDC-based watch path delivers events within ~100-500ms of commit (dependent on resolved timestamp interval configuration).
- The polling fallback has latency bounded by the poll interval (default 250ms).
- Both are acceptable given that the watch cache (`pkg/storage/cacher/`) already buffers events.

### Memory

- The TiKV Go client maintains connections to PD and TiKV stores. Connection pooling is handled by gRPC. Memory overhead per kube-apiserver is typically 50-100 MiB for the client.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **TiKV CDC API stability** | Watch implementation may break across TiKV versions | Pin to stable TiKV releases; implement polling fallback; maintain integration tests |
| **TSO as resourceVersion** | TSO timestamps are large (18-digit integers); some clients may not handle uint64 properly | Kubernetes already uses `uint64` for resourceVersion internally; wire format is string |
| **TTL accuracy** | Background reaper has ±10s granularity | Acceptable for Kubernetes Events (default TTL is 1 hour); configurable interval |
| **Operational complexity** | TiKV requires PD + TiKV stores (minimum 3+3 processes) | Provide Helm chart and operator; TiDB Operator (existing CNCF project) manages TiKV |
| **Go client maturity** | `client-go/v2` is less battle-tested than etcd client | Extensive integration testing; contribute upstream fixes; fallback to etcd is always available |
| **Transaction overhead** | 2PC adds latency vs. etcd's single-round-trip writes | Most Kubernetes operations are single-key; 2PC overhead is <2ms for single-key in same region |
| **Data model mismatch** | etcd has native TTL/lease; TiKV does not | TTL manager emulation (see above); well-tested in integration suite |

---

## Alternatives Considered

### 1. Use TiKV RawKV API Instead of TxnKV

**Rejected.** RawKV does not provide MVCC timestamps, which are essential for implementing `resourceVersion` semantics. Without MVCC, we cannot provide consistent point-in-time reads or implement the watch protocol.

### 2. Use TiKV with an External Changelog (e.g., Kafka)

**Rejected.** Adding Kafka introduces operational complexity. TiKV's CDC interface provides change events natively without an external system.

### 3. Use FoundationDB

FoundationDB is another distributed KV store with strong consistency. However, TiKV was chosen because:
- TiKV is a CNCF graduated project, aligned with the Kubernetes ecosystem.
- TiKV's Go client is more mature for this use case.
- TiKV's multi-raft architecture maps naturally to Kubernetes' key-prefix-based workload distribution.

### 4. Implement a Generic Storage Backend Plugin Interface

A plugin interface (e.g., gRPC-based) would allow arbitrary backends. This was considered but deferred because:
- The `storage.Interface` is already the abstraction boundary.
- A gRPC plugin layer adds serialization overhead and operational complexity.
- It's better to prove the concept with a concrete, high-quality implementation first.

---

## Implementation Phases

### Phase 1: Core Implementation (8 weeks)

- [ ] Add `StorageTypeTiKV` constant and factory wiring
- [ ] Implement `tikv.store` with `Create`, `Get`, `GetList`, `Delete`, `GuaranteedUpdate`
- [ ] Implement `tikv.versioner` (TSO ↔ resourceVersion mapping)
- [ ] Implement key encoding with `prepareKey()`
- [ ] Implement `value.Transformer` integration (encryption at rest)
- [ ] Unit tests for all CRUD operations
- [ ] Integration tests against `tiup playground`

### Phase 2: Watch + TTL (6 weeks)

- [ ] Implement CDC-based watcher
- [ ] Implement polling-based watcher (fallback)
- [ ] Implement `SendInitialEvents` support
- [ ] Implement TTL manager with background reaper
- [ ] Implement bookmark event emission
- [ ] Watch integration tests

### Phase 3: Operations + Observability (4 weeks)

- [ ] Implement GC safepoint management (`compact.go`)
- [ ] Implement health and readiness checks
- [ ] Add Prometheus metrics
- [ ] Implement `Stats()` (object count, average size)
- [ ] Add `RequestWatchProgress()` support
- [ ] Add `GetCurrentResourceVersion()` via PD TSO

### Phase 4: Hardening + Migration (4 weeks)

- [ ] Run full Kubernetes conformance suite against TiKV backend
- [ ] Build etcd-to-TiKV migration tool
- [ ] Write operational documentation
- [ ] Performance benchmarking and tuning
- [ ] Stress testing at scale (5000 simulated nodes)

### Phase 5: Alpha Release (2 weeks)

- [ ] Feature gate: `TiKVStorageBackend` (default: disabled)
- [ ] Documentation and release notes
- [ ] KEP submission

---

## References

- [TiKV GitHub](https://github.com/tikv/tikv)
- [TiKV client-go](https://github.com/tikv/client-go)
- [TiKV Architecture](https://tikv.org/docs/deep-dive/introduction/)
- [TiCDC](https://github.com/pingcap/tiflow)
- [Kubernetes Storage Interface](https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/interfaces.go)
- [etcd3 Storage Implementation](https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3)
- [KEP-956: Watch Bookmarks](https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/956-watch-bookmark)
