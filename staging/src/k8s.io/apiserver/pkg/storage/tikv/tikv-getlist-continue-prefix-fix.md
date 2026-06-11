# Fix Spec: TiKV `GetList` continuation key is missing the storage prefix

Status: IMPLEMENTED (2026-06-10). `store.go` `GetList` and `Watch` now validate
the continue token against the prepared `/registry`-qualified prefix, and the
recursive prefix is normalised with a trailing `/` (etcd3 parity) so decode,
scan range, and encode all use the identical prefix. Build + vet clean. No unit
tests added (the tikv package has no test harness — see §7 note).
Audience: An autonomous coding agent implementing changes in the **Kubernetes
fork** that builds `isletest.azurecr.io/kube-apiserver:tikv-dev`.
Scope: `staging/src/k8s.io/apiserver/pkg/storage/tikv/store.go` only.
Companion to: [tikv-list-latency-perf-spec.md](tikv-list-latency-perf-spec.md)
(found during the 6000-ConfigMap latency investigation),
[tikv-backend-perf-prd.md](tikv-backend-perf-prd.md) (R1/R2 paging semantics).

---

## 1. TL;DR

The TiKV backend's `GetList` validates the **continue token against the raw,
un-prefixed key** instead of the prepared storage key. Because
`DecodeContinue` reconstructs the resume key as `keyPrefix + start`, the
continuation page's `startKey` comes out **missing the `/registry` prefix**
(e.g. `/configmaps/default/cm-2320` instead of
`/registry/configmaps/default/cm-2320`). That key sorts **before the entire
`/registry/…` keyspace**, so every continuation scan:

1. **re-reads unrelated resources** that sort before the target prefix
   (`/registry/clusterroles/…`, `/registry/clusterrolebindings/…`,
   `/registry/acn.azure.com/…`), trying to decode each as the wrong type, and
2. **does not resume where the previous page left off** — a correctness risk
   (duplicated / non-advancing pages), not just wasted work.

One-line fix: pass the **prepared key prefix** (the same value the emit side
already uses) to `ValidateListOptions`, exactly as the etcd3 backend does.

This was found while investigating slow LISTs over 6000 × ~1 MB ConfigMaps. It
is **not** the dominant latency cause (the ~1 MB object bodies are — see the
latency spec), but it is a real correctness + wasted-CPU bug surfaced by the same
workload, and the fix is tiny.

---

## 2. Background: how continue tokens are prefixed

Kubernetes continue tokens store a **prefix-relative** start key, and the
storage layer re-attaches the prefix on decode. From
`staging/src/k8s.io/apiserver/pkg/storage/continue.go`:

```go
// EncodeContinue stores the key with the prefix stripped:
func EncodeContinue(key, keyPrefix string, resourceVersion int64) (string, error) {
    nextKey := strings.TrimPrefix(key, keyPrefix)
    ...
}

// DecodeContinue re-attaches the prefix to rebuild the absolute key:
func DecodeContinue(continueValue, keyPrefix string) (fromKey string, rv int64, err error) {
    ...
    return keyPrefix + cleaned[1:], c.ResourceVersion, nil
}
```

`storage.ValidateListOptions(keyPrefix, …)` calls `DecodeContinue(continue,
keyPrefix)`. So **the `keyPrefix` passed to `ValidateListOptions` must be the
same prefix used to build the scan range** — the prepared storage key
(`/registry/<resource>…`), not the API-relative key (`/<resource>…`).

The etcd3 backend does exactly this
(`staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go`):

```go
keyPrefix, err := s.prepareKey(key)          // "/registry/configmaps/"
...
withRev, continueKey, err := storage.ValidateListOptions(keyPrefix, s.versioner, opts)
```

## 3. The bug (code-grounded)

In `tikv/store.go` `GetList`, the prepared key is computed but **the raw `key`
is passed to `ValidateListOptions`**:

```go
preparedKey, err := s.prepareKey(key)        // e.g. "/registry/configmaps"
if err != nil {
    return err
}

// BUG: validates/decodes the continue token against the RAW key ("/configmaps"),
// not preparedKey ("/registry/configmaps").
withRev, continueKey, err := storage.ValidateListOptions(key, s.versioner, opts)
if err != nil {
    return err
}

startKey := preparedKey
if continueKey != "" {
    startKey = []byte(continueKey)           // <- mis-prefixed on continuation pages
}

var endKey []byte
if opts.Recursive {
    endKey = prefixEnd(preparedKey)          // correct: "/registry/configmaps"+1
} else {
    endKey = append(cloneBytes(preparedKey), 0x00)
}
```

`pathPrefix` always ends in `/` and `prepareKey` returns `pathPrefix + key` (so
`preparedKey = "/registry/configmaps"`). Meanwhile the **emit** side already
encodes the token against `preparedKey` (later in the same function):

```go
continueToken, err := storage.EncodeContinue(string(lastIterKey)+"\x00", string(preparedKey), int64(scanTS))
```

So encode uses `preparedKey` but decode uses raw `key` — an **asymmetry**.

### What goes wrong on continuation pages

1. Page 1 (no continue token): `continueKey == ""`, so `startKey = preparedKey`
   = `/registry/configmaps`. **Correct** — page 1 is fine.
2. Page 1 emits a token with prefix-relative start `/<ns>/cm-2320` (encoded
   against `preparedKey`).
3. Page 2 sends that token. `ValidateListOptions(key="/configmaps", …)` →
   `DecodeContinue("…", "/configmaps")` → returns `"/configmaps" + "/<ns>/cm-2320"`
   = **`/configmaps/<ns>/cm-2320`** (no `/registry`).
4. `startKey = "/configmaps/<ns>/cm-2320"`. Since `"/c…" < "/r…"`, this sorts
   **before all `/registry/…` keys**.
5. The scan range becomes `[/configmaps/<ns>/cm-2320, prefixEnd(/registry/configmaps))`,
   which **starts before the whole `/registry` keyspace** and walks forward
   through every resource that sorts before `/registry/configmaps/`
   (`acn.azure.com`, `clusterrolebindings`, `clusterroles`, …) and then **re-enters
   configmaps from the beginning** rather than resuming at `cm-2320`.

## 4. Evidence (live cluster, ns `6a299d4ce0495e0001c92eb6`, 6000 ConfigMaps)

apiserver runs `--v=4`, which logs the backend's per-list summary and per-key
decode errors:

- **Foreign-key decode spam while listing configmaps** — 3,140 errors in 120 s,
  exclusively for resources that sort *before* configmaps:

  ```
  tikv GetList: decode error for key /registry/clusterroles/system:node:
    converting (v1.ClusterRole) to (core.ConfigMap): unknown conversion
  ```
  Distribution: `clusterroles` 1680, `clusterrolebindings` 1440,
  `acn.azure.com` 20 — all lexically `< /registry/configmaps/`.

- **Over-scan in the completion line**: each `limit=500` page reported
  `returned=500 scannedKeys=690` — ~190 non-configmap keys visited per
  continuation page:

  ```
  "tikv GetList complete" resource="configmaps" key="/configmaps" limit=500
    returned=500 scannedKeys=690 scannedBytes=524728294 pages=3 elapsed="3.5s"
  ```

- The `httplog` showed the continuation request whose token start was
  `/default/cm-2320`, i.e. the token itself is well-formed — only the **decode
  prefix** is wrong.

## 5. Impact

- **Correctness (primary concern)**: continuation pages do not resume at the
  intended key; the mis-positioned `startKey` re-enters the target prefix from
  the start, risking **duplicate items and non-terminating / wrong pagination**
  for any paged consumer (reflectors using `WatchListPageSize`, `kubectl
  --chunk-size`, controllers walking continue tokens).
- **Performance (secondary)**: wasted scan + failed decode of every resource
  that sorts before the target prefix, on **every continuation page**. CPU/log
  overhead scales with how many resource prefixes precede the target
  alphabetically. Byte volume is small relative to the ~1 MB object bodies, so
  this is **not** the main LIST latency driver — but it is pure waste.
- Scope: affects **every recursive paged LIST** on the TiKV backend (any
  resource, any namespace) once a continue token is in play. Single-page lists
  (no continue) and the first page are unaffected.

## 6. The fix

Pass the **prepared key prefix** to `ValidateListOptions`, matching the emit side
and the etcd3 backend.

```go
preparedKey, err := s.prepareKey(key)
if err != nil {
    return err
}

// Validate/decode the continue token against the SAME prefix used to build the
// scan range and to encode the token (preparedKey), so the continuation start
// key is fully /registry-qualified. Passing the raw key drops the storage
// prefix and mis-positions the scan before the /registry keyspace.
withRev, continueKey, err := storage.ValidateListOptions(string(preparedKey), s.versioner, opts)
if err != nil {
    return err
}
```

Notes (as implemented):
- This is symmetric with the existing `EncodeContinue(..., string(preparedKey), ...)`
  emit call — encode and decode now use the identical prefix.
- The same change was applied to `Watch`: it now computes `preparedKey` first and
  validates with `string(preparedKey)`. It discards `continueKey` today so this
  is harmless now, but removes the latent trap if Watch ever honors continue.
- **Recursive trailing `/` (etcd3 parity) — APPLIED, not optional.** For a
  recursive LIST `preparedKey` is normalised to end in `/`
  (`if opts.Recursive && !bytes.HasSuffix(preparedKey, []byte("/")) { preparedKey
  = append(cloneBytes(preparedKey), '/') }`) BEFORE `ValidateListOptions`, so the
  prefix can't match a sibling like `/registry/configmapsX`, and decode / scan
  range (`prefixEnd(preparedKey)`) / encode all share the **same** slash-suffixed
  prefix string. This is what keeps the three in lockstep — the original bug was
  precisely that they diverged.

## 7. Test plan

> **NOT DONE — no harness.** The tikv package has no `*_test.go` files and no
> mocktikv/unistore harness wired up (consistent with the prior PERF rounds).
> Standing up one is out of scope for this tiny fix. Validate instead via the
> live-cluster repro in §4 / §8 (kperf 6000 ConfigMaps): zero foreign-resource
> `unknown conversion` decode errors during a paged configmaps LIST, and
> `scannedKeys == returned` per page absent predicate filtering. The plan below
> is retained for when a harness exists.

1. **Unit (`storage/tikv/store_test.go`)** — seed keys for **two adjacent
   resources** whose prefixes sort next to each other (e.g. `configmaps` and a
   resource that sorts just before it), plus N configmaps. Then:
   - Page through configmaps with `limit` < N using the returned continue tokens.
   - Assert: (a) the **union of pages == the full set with no duplicates**, (b)
     **page 2+ start at the expected key** (no restart from the beginning), and
     (c) **no foreign-resource keys are visited** (instrument the scan / assert
     `scannedKeys == returned` when there is no predicate filtering).
2. **Continue-prefix regression** — craft a continue token for a paged list and
   assert the decoded `startKey` begins with `pathPrefix` (`/registry/…`).
3. **Parity with etcd3** — port the relevant `TestListContinuation` /
   `TestListContinuationWithFilter` cases and run them against the TiKV store.

## 8. Acceptance

| Check | Pass condition |
|-------|----------------|
| Continuation prefix | decoded continuation `startKey` is `/registry/…`-qualified |
| No over-scan | paged LIST visits **only** the target resource prefix; `scannedKeys == returned` absent predicate filtering |
| No duplicates / progress | walking all pages yields each object exactly once and terminates |
| No foreign decode errors | zero `unknown conversion` decode errors during a configmaps LIST |
| etcd3 parity | ported continuation tests pass on the TiKV store |

## 9. Key file references (fork)

- `staging/src/k8s.io/apiserver/pkg/storage/tikv/store.go` — `GetList`
  `ValidateListOptions(key, …)` (≈619, **the fix**), `EncodeContinue(…,
  string(preparedKey), …)` emit (≈697), `prepareKey` (≈225), `Watch`
  `ValidateListOptions(key, …)` (≈496, same change for consistency).
- `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go` — reference: validates
  with the prepared `keyPrefix` (≈757) and adds the recursive trailing `/`.
- `staging/src/k8s.io/apiserver/pkg/storage/continue.go` — `EncodeContinue` /
  `DecodeContinue` (prefix attach/strip), `ValidateListOptions`
  (`interfaces.go` ≈346).
