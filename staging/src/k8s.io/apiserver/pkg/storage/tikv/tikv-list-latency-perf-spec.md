# kube-apiserver LIST latency on the TiKV backend: Perf Spec

Status: Draft (implementation-ready)
Audience: An autonomous coding agent improving LIST/GET latency in the
**Kubernetes fork** that builds `isletest.azurecr.io/kube-apiserver:tikv-dev`,
plus the AKS control-plane config that fronts it.
Companion to: [tikv-list-oom-rca.md](tikv-list-oom-rca.md) (the OOM, now fixed)
and [tikv-backend-perf-prd.md](tikv-backend-perf-prd.md).

> Context shift: the relist no longer **OOMKills** the apiserver (container limit
> raised to 50G; watch-cache seeding survives). The remaining problem is
> **latency** — a default `kubectl get configmaps` against a large collection
> takes **tens of seconds to ~1.5 minutes**. This doc quantifies where that time
> goes and specifies the fixes, ranked by measured impact.

---

## 1. TL;DR

Measured on the live cluster (CCP ns `6a299d4ce0495e0001c92eb6`, kube-apiserver
`--storage-backend=tikv`, **2196 ConfigMaps ≈ 2.29 GB**, ~1 MB each):

| Request | Bytes | TTFB | Total |
|---------|-------|------|-------|
| Full list, **RV=0, JSON** (default `kubectl get cm`) | 2.29 GB | **27.4 s** | **90.2 s** |
| Full list, **RV=0, protobuf** | 2.29 GB | 0.06 s | 46.4 s |
| Page **limit=200, JSON** | 210 MB | 4.73 s | 8.36 s |
| Page **limit=200, protobuf** | 210 MB | 1.98 s | 6.14 s |
| **Metadata-only** (PartialObjectMetadata), limit=200 | **188 KB** | 1.90 s | 1.93 s |
| Page limit=10, JSON | ~10 MB | — | 1.14 s |
| Page limit=1, JSON | ~1 MB | — | 0.86 s |

Four independent latency levers, in priority order:

1. **P1 — `limit` is ignored on cache reads (`RV=0`).** A default list returns
   the **entire 2.29 GB collection**, not a page. This is the dominant cost
   (90 s). Honor pagination on the watch-cache read path. **~90 s → <1 s/page.**
2. **P2 — JSON server-side encoding is ~2× protobuf and not streamed.** JSON adds
   **27.4 s of TTFB** on the full set (protobuf 0.06 s). Prefer protobuf for
   in-cluster clients and stream the encoder.
3. **P3 — Bodies dominate; no projection pushdown.** Metadata is **188 KB vs
   210 MB** (1115× smaller) yet still costs 1.9 s because the apiserver fetches +
   decodes full 1 MB bodies before projecting. Push metadata/field projection
   down to storage.
4. **P4 — Transfer + per-replica memory.** 2.29 GB on the wire is ~46 s even at
   protobuf best-case; and every apiserver replica holds the collection resident
   (**RSS ~8.2–9.3 GB/replica**). Add response compression and cap watch-cache by
   bytes.

---

## 2. Environment & method

- Cluster: standalone CCP, customer apiserver via `~/.kube/kperf`, namespace
  `6a299d4ce0495e0001c92eb6`.
- Backend: PD ×3 + TiKV ×3, all `Running`, **0 restarts** (storage layer healthy
  — this is purely an apiserver-path latency study, not a TiKV-availability one).
- apiserver: 4 replicas, container memory **limit 50G**, steady-state RSS
  **8.2–9.3 GB/replica**.
- Dataset: `default` namespace, **2196 ConfigMaps**, **~2.29 GB** total, ~1 MB
  each (a `kperf` load).
- Measurement: `curl` with the admin client cert, `-w` capturing
  `size_download`, `time_total`, `time_starttransfer` (TTFB). TTFB ≈ server-side
  "produce the first byte" (compute/encode); `total − TTFB` ≈ transfer. Server
  metrics from `/metrics` (`apiserver_response_sizes`, `apiserver_request_duration_seconds`).

All numbers in §1 are from single runs on the warm cluster; they are large and
reproducible enough to drive prioritization (the 10–100× gaps are not noise).

---

## 3. Where the time goes (analysis)

### 3.1 P1 — `limit` ignored on `RV=0` ⇒ full-collection response (dominant)

The biggest cost is that the **default** list shape returns everything. With
`resourceVersion=0` (what client-go/reflectors and a plain `kubectl get` use to
read from the watch cache), the cacher **drops the limit**:

`staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go` → `computeListLimit`:

```go
// as of today, the limit is ignored for requests that set RV == 0
func computeListLimit(opts storage.ListOptions) int64 {
    if opts.Predicate.Limit <= 0 || opts.ResourceVersion == "0" {
        return 0   // <-- returns the entire collection
    }
    return opts.Predicate.Limit
}
```

Evidence: the `RV=0` request returned **2.29 GB / 2196 objects** (90.2 s JSON /
46.4 s protobuf), whereas `limit=200` returned one 210 MB page in 6–8 s and
`limit=1` in 0.86 s. The watch cache itself is **warm and fast** (protobuf
`RV=0` TTFB = **0.06 s** — the cache produces the snapshot instantly); the time
is spent **encoding and shipping the whole collection**.

So a single client that lists without an explicit `limit` (the default for
`kubectl get`, many controllers, and every reflector resync) pays the full
2.29 GB cost and ties up an apiserver worker for ~90 s.

### 3.2 P2 — JSON encoding is ~2× protobuf and blocks TTFB

Comparing equal payloads:

- limit=200: protobuf TTFB **1.98 s** vs JSON **4.73 s** (2.4×).
- RV=0 full set: protobuf TTFB **0.06 s** vs JSON **27.4 s**.

The protobuf cache read streams almost immediately (the cache stores objects in a
near-wire form), while **JSON must re-encode all 2196 objects before the first
byte** — 27.4 s of pure CPU serialization, single-request. Total time follows:
protobuf 46.4 s vs JSON 90.2 s (≈2×).

Two issues compound here: (a) JSON is the costly codec, and (b) the encoder is
**not incremental** — TTFB ≈ "encode the whole list," so a slow client or a large
list serializes entirely in memory before streaming.

### 3.3 P3 — bodies are 99.9% of bytes; projection isn't pushed down

`PartialObjectMetadata` (names/labels/annotations only) for the same 200 objects
was **188 KB vs 210 MB** — bodies are **99.9%** of the payload. Yet the
metadata-only request still took **1.9 s**, because the apiserver **fetches and
decodes the full 1 MB ConfigMap bodies from storage and then strips them**. A
client that only needs names (e.g. a GC/labeling controller, `kubectl get` for a
table) still forces the backend to read and decode gigabytes.

`apiserver_response_sizes` for configmaps WATCH already shows **976 MB across 15
watches** — the body bloat hits the watch path too.

### 3.4 P4 — transfer + per-replica resident memory

Even the best case (protobuf, full set) is **46 s**, almost entirely transfer of
2.29 GB (TTFB 0.06 s). And the watch cache holds the whole collection **resident
in every replica**: RSS **8.2–9.3 GB/replica** for a 2.29 GB dataset (≈3–4× for
decoded Go objects), ×4 replicas. Large-object collections therefore inflate
every apiserver's memory footprint and the wire cost of every full read.

---

## 4. Fixes (prioritized, with acceptance)

### P1 — Paginate cache reads; stop returning whole collections (MUST)

Make the watch-cache LIST honor `limit` and emit a `continue` token, including
the `RV=0` / consistent-read-from-cache path, so a default list returns one
bounded page instead of the entire collection.

- In `cacher.go`, stop forcing `limit = 0` for `RV=0` when the client supplied a
  positive limit; thread the limit and a continue token through the cache read
  (the cache already indexes by key order, so it can return `[start, start+limit)`
  and a token). Where `ListFromCacheSnapshot` / `ConsistentListFromCache` exist
  (`kube_features.go`), use the snapshot to serve consistent paged reads.
- For the TiKV storage path, the `RV=0`→`limit=0` request also reaches the
  backend unpaginated; the backend already pins a snapshot and tracks a continue
  token but only emits it when `limit > 0` (see RCA §4.2). Apply an **internal
  page size** so even an unpaginated relist is produced in bounded chunks
  (RCA F2).
- Make clients page: ensure reflectors/informers and AKS control-plane
  controllers set `WatchListPageSize` / a default `--chunk-size`, and document
  that bulk `kubectl get` on large collections should use `--chunk-size`.

Acceptance:
- A default `LIST configmaps` (no explicit limit) returns a **single bounded
  page** (≤ pageSize objects) with a continue token; p99 **< 1 s** at 2196×1 MB.
- Walking all pages returns the same set as today; total bytes unchanged but no
  single response exceeds `pageSize × maxObjectBytes`.

### P2 — Stream the encoder and prefer protobuf for in-cluster clients (SHOULD)

- **Streaming encode/flush**: emit and flush list items incrementally rather than
  serializing the whole list before first byte, so TTFB and peak encode memory
  are per-item, not per-collection. Targets the 27.4 s JSON TTFB.
- **Protobuf by default for control-plane clients**: ensure in-cluster clients
  (KCM, scheduler, addons, AKS controllers) negotiate
  `application/vnd.kubernetes.protobuf`; audit any client pinned to JSON. ~2×
  on both TTFB and total for large lists.

Acceptance:
- For a fixed page, JSON TTFB is within ~1.3× of protobuf (not 2.4×) after
  streaming; full-set JSON TTFB drops from 27 s to single-digit seconds.
- Control-plane client traffic is ≥ 90% protobuf by `apiserver_response`
  content-type.

### P3 — Projection pushdown for metadata/field-selector reads (SHOULD)

When the request is `PartialObjectMetadata`/Table or restricts fields, avoid
fetching+decoding full bodies in the TiKV backend.

- Plumb a "metadata-only" hint into `storage.Interface`/the TiKV `GetList` so it
  can decode only `ObjectMeta` (or skip the `data`/`binaryData` of ConfigMaps,
  the `data` of Secrets) instead of the whole object.
- At minimum, decode lazily so stripped fields are never materialized.

Acceptance:
- A metadata-only LIST of 2196 ConfigMaps transfers < 5 MB **and** the backend
  reads/decodes < 50 MB (not 2.29 GB); p99 **< 1 s**.

### P4 — Response compression + bytes-bounded watch cache (NICE)

- **Transport compression** (gzip) for large list responses to cut the
  transfer-bound tail (46 s of 2.29 GB). Negotiated via `Accept-Encoding`.
- **Cap the watch cache by bytes** (not just object count) and enforce a
  **max object size** (RCA F1) so a few-thousand 1 MB objects can't pin
  ~9 GB/replica; oversize objects are rejected at write time.

Acceptance:
- Compressed full-set transfer is ≤ 50% of uncompressed wall time for
  configmaps.
- apiserver RSS for the collection is bounded by the configured cache byte cap;
  an over-cap object write is rejected (413) and never enters the cache.

---

## 5. Measurement plan (repeatable)

Use the same harness to verify each fix (admin cert from the kperf kubeconfig):

```bash
SRV=<apiserver-url>
# per-page latency + bytes, JSON vs protobuf
for acc in application/vnd.kubernetes.protobuf application/json; do
  curl -sk --cert c.crt --key c.key -H "Accept: $acc" \
    -w 'bytes=%{size_download} total=%{time_total}s ttfb=%{time_starttransfer}s\n' \
    -o /dev/null "$SRV/api/v1/namespaces/default/configmaps?limit=200"
done
# default (RV=0) full-collection cost — should become one page after P1
curl -sk ... "$SRV/api/v1/namespaces/default/configmaps?resourceVersion=0"
# metadata-only — should become cheap end-to-end after P3
curl -sk -H 'Accept: application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1' \
  ... "$SRV/api/v1/namespaces/default/configmaps?limit=200"
```

Server-side signals: `apiserver_request_duration_seconds{verb="LIST",resource="configmaps"}`
(p50/p99), `apiserver_response_sizes{...}`, and apiserver container RSS
(`kubectl top pod --containers`). Load generator: `kperf` at 2000–5000 × ~1 MB
ConfigMaps.

### Target SLOs (post-fix)

| Scenario | Today | Target |
|----------|-------|--------|
| Default `get configmaps` (one page) | 90 s (whole set) | **< 1 s** (P1) |
| Full enumeration, paged, protobuf | n/a (single 46–90 s call) | **< 1 s/page**, linear |
| Metadata-only list (2196 objs) | 1.9 s + 210 MB read | **< 1 s**, < 5 MB (P3) |
| apiserver RSS / replica @ 2.3 GB data | 8.2–9.3 GB | bounded by cache byte cap (P4) |

---

## 6. Key file references (fork)

- `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go` —
  `computeListLimit` (≈689) **(P1)**, cache read/paging path.
- `staging/src/k8s.io/apiserver/pkg/storage/tikv/store.go` — `GetList` (≈530),
  internal-page on `limit==0` **(P1/P3)**, metadata-only decode **(P3)**.
- `staging/src/k8s.io/client-go/tools/cache/reflector.go` — `WatchListPageSize`
  / `pager.PageSize` (≈601) **(P1)**.
- `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/` (response encoding /
  content negotiation) — streaming encoder, protobuf default **(P2)**.
- `staging/src/k8s.io/apiserver/pkg/features/kube_features.go` — `WatchList`,
  `ListFromCacheSnapshot`, `ConsistentListFromCache` **(P1/P2)**.

---

## 7. Relationship to the other docs

- The **OOM** (apiserver killed during relist) is covered by
  [tikv-list-oom-rca.md](tikv-list-oom-rca.md); its **F2 (bounded/streamed relist)**
  and **F1 (size caps)** overlap with **P1** and **P4** here — the same
  unpaginated-full-collection root cause produces *both* the OOM (when undersized)
  and the latency (when sized up to 50G). Fixing P1 fixes both.
- The broader backend requirements (continue-token semantics, snapshot
  consistency, Count) are in
  [tikv-backend-perf-prd.md](tikv-backend-perf-prd.md) (R1–R6).
