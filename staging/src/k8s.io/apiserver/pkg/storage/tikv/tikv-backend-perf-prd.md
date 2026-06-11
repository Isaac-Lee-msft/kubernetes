# PRD: kube-apiserver TiKV storage-backend performance & memory safety

Status: Draft (POC follow-up)
Owner: AKS CCP / kube-apiserver storage POC
Audience: An autonomous coding agent implementing changes in the **kubernetes
fork** that produces `isletest.azurecr.io/kube-apiserver:tikv-dev`.

> This PRD covers ONLY the apiserver-side storage backend
> (`k8s.io/apiserver/pkg/storage/tikv`). The Kubernetes-side deployment/chart
> hardening (resource limits, `tikv.toml` memory bounds, PDBs, anti-affinity,
> probes) is implemented separately in
> [`templates/tikv-cluster.yaml`](../templates/tikv-cluster.yaml) and is NOT in
> scope here.

---

## 1. Background

The CCP POC replaces etcd with TiKV as the kube-apiserver storage backend. The
apiserver runs with:

```
--storage-backend=tikv
--etcd-servers=https://etcd-<CCPID>-client.<ns>.svc.cluster.local:2379   # PD client endpoint
--etcd-cafile/-certfile/-keyfile=/etc/kubernetes/etcd-client-tls/...      # mTLS to PD
```

PD (3 replicas) provides TSO/metadata; TiKV (3 replicas) is the data plane. The
apiserver talks to PD/TiKV via `tikv/client-go`.

The backend implementation lives in the fork at:

```
staging/src/k8s.io/apiserver/pkg/storage/tikv/
  store.go        # storage.Interface impl (Get/GetList/Create/Delete/GuaranteedUpdate/Count)
  watcher.go      # storage.Interface Watch
  ...
```

Use the etcd3 backend (`staging/src/k8s.io/apiserver/pkg/storage/etcd3/`) as the
reference implementation for semantics.

## 2. Problem statement

A perf test loaded **1000+ large ConfigMaps** into one cluster. The TiKV pods
were then **OOMKilled on every replica simultaneously**, losing raft quorum, and
the apiserver entered a mutual crash loop with TiKV. Root cause analysis:

1. **`GetList` performs an unbounded full-prefix scan.** A `LIST configmaps`
   (including the apiserver's own watch-cache **relist** at startup, which sends
   no `limit`) materializes the entire key range for the resource in a single
   pass. Memory scales with the *total bytes of the resource*, not a bound.
2. **`limit` / `continue` pagination is ignored.** Even an explicit
   `?limit=20&continue=...` request scanned the whole keyspace (observed: a
   20-item page hung for minutes against ~1.4 GB of data).
3. **`Count()` transfers values.** The per-resource object-count tracker
   (`store.go: "Monitoring resource count at path"`) triggers a scan that pulls
   values, not just keys.
4. The large serialized result set is buffered in TiKV's gRPC layer (not bounded
   by `memory-usage-limit`) and decoded into Go objects in the apiserver (5–7×
   blow-up), exceeding both the TiKV container limit and the apiserver
   container limit (3 GB).

Net effect: a single tenant action (bulk create) makes the control plane
**unrecoverable** on realistically-sized nodes, because the apiserver triggers
the OOM the moment it starts, yet the apiserver is the only way to delete the
offending data.

### Reproduction

- Standalone cluster (Dev AKS Deploy), `--storage-backend=tikv`.
- `kperf` (or a loop) creating ~1000–5000 ConfigMaps of ~1 MB each in `default`.
- Observe TiKV `OOMKilled` (exit 137) and apiserver crash loop.
- Confirmed mitigations that proved the diagnosis:
  - `--watch-cache=false` stops the startup relist → TiKV stays flat (~1.4 GiB),
    proving the relist scan is the trigger.
  - A normal `LIST default configmaps` took **minutes** with the data present,
    **0.39s** after deletion.

## 3. Goals / non-goals

### Goals

- G1. A `LIST` (paged or unpaged) executes in **bounded memory** on both TiKV
  and the apiserver, independent of total resource size.
- G2. `limit` + `continue` pagination works and is **O(page)**, not O(prefix).
- G3. `Count()` is cheap (keys-only, no value transfer).
- G4. The apiserver **watch-cache relist** never materializes a whole resource;
  it streams/pages internally even when the caller sets no `limit`.
- G5. Semantics match etcd3 closely enough that reflectors/informers, the
  pager, and `kubectl` paging work unchanged.

### Non-goals

- Changing the TiKV/PD topology, TLS, or the Helm chart (done elsewhere).
- Watch performance optimization beyond what G4 requires (separate effort).
- Transactions/consistency model changes beyond what LIST snapshotting needs.

## 4. Functional requirements

### R1 — Paginated `GetList` honoring `limit` and `continue`

Mirror `etcd3/store.go: GetList`.

- When `opts.Predicate.Limit > 0`, issue a **bounded** TiKV scan
  (`txnkv` snapshot `Iter(startKey, endKey)` consumed up to a page size, or a
  coprocessor/`Scan` with limit) and stop once `limit` **accepted** items are
  collected.
- When a field/label predicate filters items, keep fetching **additional bounded
  pages** internally (loop) until `limit` accepted items or the prefix is
  exhausted — never widen a single scan to the whole prefix.
- Produce a `continue` token when more items remain. **Reuse the etcd3 continue
  token format** so the apiserver pager and clients interoperate:
  - `etcd3` encodes base64(JSON{`apiVersion`, `resourceVersion`, `startKey`}).
  - Decode/validate exactly like
    `etcd3.decodeContinue(continue, keyPrefix)` and reject mismatched prefixes
    with `storage.NewInvalidError`.
- Set `list.ResourceVersion`, `list.Continue`, and `RemainingItemCount` (when
  cheaply known) on the returned list.

Acceptance:
- `LIST ?limit=500` returns exactly 500 items + a continue token; the underlying
  TiKV scan transfers ≈500 rows, not the whole prefix.
- Walking all pages yields the same set as etcd3 for an identical dataset.
- `TestListContinuation`, `TestListPaginationRareObject`,
  `TestListContinuationWithFilter` (ported from etcd3) pass.

### R2 — Snapshot consistency across pages

- A multi-page LIST must read at a **single MVCC snapshot**. Pin the snapshot
  timestamp (TSO) into the continue token and reopen the snapshot at that `ts`
  for each subsequent page (etcd3 pins the etcd revision the same way).
- Map `ResourceVersionMatch`:
  - `""` (default for relist) → latest committed snapshot (current TSO).
  - `NotOlderThan` → a snapshot `>=` the requested RV.
  - `Exact` → the snapshot at that RV (used by `continue`).
- The RV value exposed to clients is the TiKV snapshot `ts` (already the scheme
  used by the backend — keep it consistent for Get/List/Watch).

Acceptance:
- Creating objects during a paged list does **not** cause items to appear/vanish
  mid-pagination (snapshot isolation).
- `TestListResourceVersion`-style tests pass.

### R3 — Internally chunked scans for unpaginated LIST (the relist fix)

This is the specific fix for the OOM.

- When the caller sets **no** `limit` (e.g. the watch-cache relist, or
  `kubectl get` without `--chunk-size`), the backend MUST still iterate the
  prefix in **bounded internal pages** (default page size configurable, e.g.
  `listPageSize = 1000` keys or a byte budget), accumulating results
  incrementally and releasing each page's raw bytes before fetching the next.
- Enforce a **byte budget per scan RPC** so a single gRPC message can't balloon;
  rely on the server-side `server.grpc-memory-pool-quota` AND a client-side cap.
- Optional but recommended: when an unpaginated list exceeds a configurable
  **soft cap** (count or bytes), return
  `storage.NewTooLargeResourceVersionError`-style guidance is NOT appropriate;
  instead transparently page internally (preferred) so callers are unaffected.

Acceptance (primary incident metric):
- A `LIST configmaps` over **5000 objects totaling ~1.5 GiB** completes with:
  - apiserver container peak RSS **< 2 GiB** (limit 3 GB),
  - each TiKV pod peak **< 4 GiB** (limit 8 GiB),
  - **zero** OOMKills,
  - p99 internal page latency **< 500 ms**.
- With `--watch-cache=true` (default), apiserver startup relist completes and
  the apiserver reaches `Ready` without OOM on a 12.5 GiB node.

### R4 — Cheap `Count()`

Mirror `etcd3/store.go: Count` (which uses a `CountOnly` range).

- Implement Count via a **keys-only** scan (no value transfer): iterate the
  prefix counting keys, or use a TiKV API that returns counts without values.
- Never decode or transfer values for Count.

Acceptance:
- `Count()` over 10k large ConfigMaps transfers **no values** and uses
  **< 100 MiB** transient memory; `TestCount`-style test passes.

### R5 — Resource-version & error mapping parity

- Map TiKV/PD errors to `k8s.io/apiserver/pkg/storage` errors consistently:
  - not-found → `storage.NewKeyNotFoundError`
  - conflict/CAS failure → `storage.NewResourceVersionConflictsError`
  - too-old RV / compacted snapshot → `apierrors.NewResourceExpired` +
    `storage.NewInvalidError` for bad continue tokens
  - `NotLeader`/region-cache-miss/`EpochNotMatch` → **retry with backoff**
    inside the backend (transparent), not surfaced as a 500.
- Ensure `GuaranteedUpdate` retries are **bounded** and do not spin
  (previous POC bug: "GuaranteedUpdate exceeded max retries" / infinite hang).

### R6 — Observability

Add Prometheus metrics + structured logs (namespaced
`apiserver_storage_tikv_*`):

- `list_pages_total`, `list_scanned_bytes`, `list_returned_objects`,
  `list_duration_seconds` (histogram), `count_duration_seconds`,
  `guaranteed_update_retries_total`, `not_leader_retries_total`.
- A `klog` V(4) line per LIST: prefix, limit, pages, bytes, objects, snapshot
  ts, elapsed.

These are required to verify R1–R4 and to catch regressions in CI.

## 5. Design notes

- **Key layout**: keep the existing `/registry/<resource>/<ns>/<name>` prefix
  scheme so range bounds are `[prefix, prefixEnd)` (prefix + 0xFF) exactly like
  etcd3. Do not change the key encoding (it would invalidate existing data).
- **Iteration**: prefer `txnkv` snapshot `Iter` with an explicit upper bound and
  a `for i := 0; i < pageSize && it.Valid(); i++` loop, calling `it.Next()` and
  copying out `it.Key()/it.Value()` then releasing. Close iterators promptly.
- **Continue token**: implement `encodeContinue(lastKey, keyPrefix, ts)` /
  `decodeContinue` byte-compatible with etcd3 so mixed tooling works.
- **Page size**: expose `listPageSize` and `listMaxBytesPerScan` as backend
  config (plumb through `storagebackend.Config`/`TransportConfig` or a
  package-level default with an env override) so they can be tuned without a
  rebuild.
- **Predicate push-down (optional, later)**: field selectors on
  `metadata.namespace`/`metadata.name` can be turned into tighter key ranges to
  avoid scanning unrelated namespaces. Not required for v1 but high value.

## 6. Test plan

1. **Unit (port from etcd3)** in `storage/tikv/store_test.go`:
   `TestList`, `TestListWithoutPaging`, `TestListContinuation`,
   `TestListPaginationRareObject`, `TestListContinuationWithFilter`,
   `TestListInconsistentContinuation`, `TestCount`, `TestGetListNonRecursive`,
   `TestListResourceVersion`.
2. **Memory bound test**: a Go test that writes N large objects and asserts the
   process RSS delta during a full LIST stays below a threshold (use
   `runtime.ReadMemStats` / `MaxRSS`).
3. **Integration (in this repo)**: deploy a standalone with the rebuilt
   `tikv-dev` image, run the `kperf` repro (5000 × ~1 MB ConfigMaps), and assert
   the R3 acceptance metrics via `kubectl top pod` / cgroup `memory.peak` on
   TiKV and the apiserver. Validate a relist (rollout-restart the apiserver) and
   a `kubectl get configmaps` both complete without OOM.
4. **Regression gate**: wire the unit + memory tests into the fork's CI for the
   `tikv` package.

## 7. Rollout

1. Implement R1–R6 in the fork; add tests.
2. Build & push `isletest.azurecr.io/kube-apiserver:tikv-dev` (new digest).
3. Update the deployment to the new digest (POC image override path).
4. Run the integration repro on a standalone; capture before/after memory and
   latency.
5. Record the verified digest + metrics in the POC notes.

## 8. Acceptance summary (single source of truth)

| ID | Requirement | Pass condition |
|----|-------------|----------------|
| R1 | Paged LIST | `?limit=N` transfers ≈N rows; pages reconcile to etcd3 set |
| R2 | Snapshot pages | No phantom/missing items mid-pagination |
| R3 | Unpaginated relist bounded | 5000×1MB LIST: apiserver <2GiB, TiKV <4GiB/pod, 0 OOM, p99 page <500ms |
| R4 | Cheap Count | 10k objects: no value transfer, <100MiB |
| R5 | Error/RV parity | Reflectors/informers work; bounded GuaranteedUpdate retries |
| R6 | Observability | `apiserver_storage_tikv_*` metrics emitted & asserted in CI |

## 9. Related (out-of-scope) infra recommendations

Tracked with the chart change, not this PRD:

- TiKV should run on **dedicated/larger nodes**, not co-scheduled with the
  control plane it serves (current agentpool nodes are ~12.5 GiB).
- Container memory **limit must stay below node allocatable** (clean
  per-container OOM instead of node-wide kernel OOM).
- PDBs (`minAvailable: 2`), **required** anti-affinity, startup probes, and the
  `tikv.toml` memory bounds (`memory-usage-limit`, `grpc-memory-pool-quota`,
  bounded block-cache/write-buffers) are implemented in
  [`templates/tikv-cluster.yaml`](../templates/tikv-cluster.yaml).
- A **raft-leader-aware** liveness signal (the current `tcpSocket:20160` probe
  passes even with no leader) would let Kubernetes detect a wedged store.
