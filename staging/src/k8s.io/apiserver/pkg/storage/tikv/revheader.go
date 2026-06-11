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
	"bytes"
	"encoding/binary"
	"hash/fnv"
)

// TiKV has no per-key commit-ts read-back API and exposes no etcd-style
// ModRevision predicate.  To give Kubernetes the resourceVersion semantics it
// needs -- a stable, monotonic version that is identical across repeated
// reads of an unchanged key and advances only on actual writes -- the store
// persists the PD TSO reserved for the write as an 11-byte header on the
// stored value:
//
//   bytes 0..2  : magic 0xff 0xfe 0x01
//   bytes 3..10 : BigEndian uint64 commit revision (PD TSO)
//   bytes 11..  : codec-encoded runtime.Object
//
// The header sits INSIDE the transformer envelope so encryption (e.g.
// AES-GCM) covers it too.  The magic bytes were chosen to be distinct from
// every codec preamble that runs underneath storage (`k8s\0` for the k8s
// protobuf codec, `{` for JSON, `[` for some list payloads) so values
// written by a pre-header build can be detected and handled.
//
// Backward compatibility: decodeWithRev returns (0, raw) when the magic is
// not present.  Callers MUST treat rev==0 as "rev unknown" and substitute a
// stable, deterministic resourceVersion derived from the stored bytes (see
// legacyRev) so reads of unmigrated data still produce a non-zero,
// *repeatable* resourceVersion.  Using a per-read value such as the snapshot
// StartTS here would break optimistic-concurrency callers (e.g. the IP/port
// range allocators), whose compare-and-swap requires that two reads of the
// same unchanged value report the same resourceVersion; an ever-changing rev
// makes their CAS never converge and the surrounding GuaranteedUpdate retry
// loop spin forever.  The first GuaranteedUpdate of any such key rewrites it
// with a proper header, after which the real PD TSO rev is used.

const revHeaderLen = 11

var revHeaderMagic = [3]byte{0xff, 0xfe, 0x01}

// encodeWithRev prepends the 11-byte header to data and returns a new slice.
func encodeWithRev(rev uint64, data []byte) []byte {
	out := make([]byte, revHeaderLen+len(data))
	out[0], out[1], out[2] = revHeaderMagic[0], revHeaderMagic[1], revHeaderMagic[2]
	binary.BigEndian.PutUint64(out[3:11], rev)
	copy(out[11:], data)
	return out
}

// decodeWithRev returns (rev, payload).  For values without the header it
// returns (0, raw) so callers can detect legacy data.
func decodeWithRev(raw []byte) (uint64, []byte) {
	if len(raw) < revHeaderLen || !bytes.Equal(raw[:3], revHeaderMagic[:]) {
		return 0, raw
	}
	return binary.BigEndian.Uint64(raw[3:11]), raw[revHeaderLen:]
}

// legacyRev derives a stable, non-zero resourceVersion for a legacy value
// stored without a rev header.  The value is a deterministic 64-bit hash of
// the (header-stripped) payload: it is identical across repeated reads of an
// unchanged value -- which optimistic-concurrency callers depend on -- and
// changes when the stored bytes change.  This is a transitional rev only:
// the next GuaranteedUpdate rewrites the key with a real PD-TSO header.
func legacyRev(payload []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(payload)
	rev := h.Sum64()
	if rev == 0 {
		// 0 is reserved for "key does not exist"; never report it for an
		// existing value.
		rev = 1
	}
	return rev
}
