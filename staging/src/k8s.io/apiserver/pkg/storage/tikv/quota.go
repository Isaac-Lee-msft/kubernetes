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
	"fmt"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/storage/tikv/metrics"
	"k8s.io/klog/v2"
)

// Collection quota: the write-side counterpart to listMaxBytes.
//
// etcd bounds a cluster with TWO independent guards:
//
//	--max-request-bytes   (default 1.5 MiB) caps a SINGLE object
//	--quota-backend-bytes (etcd default 2 GiB; AKS default 4 GiB, 8 GiB for
//	                       large clusters) caps the TOTAL keyspace, and once
//	                       exceeded etcd raises a NOSPACE alarm and refuses all
//	                       writes until an operator compacts and defragments.
//
// The TiKV backend already mirrors the first (see checkObjectSize) but had no
// analogue of the second, and TiKV itself imposes no keyspace cap. That gap is
// exactly what makes the relist OOM reachable: an unbounded collection is
// still WRITABLE long after it has become too large to LIST, so the cluster
// walks itself into a state where every watch-cache relist fails -- and
// because deleting the offending objects requires a working apiserver, that
// state can be unrecoverable.
//
// listMaxBytes already fails an oversized unpaginated LIST safely instead of
// OOMing. This guard closes the loop on the write side: if a collection may
// not be LISTed past N bytes, it should not be allowed to GROW past N bytes
// either. Hence the default below is listMaxBytes.
//
// Note this is deliberately a PER-COLLECTION quota rather than a global
// keyspace quota like etcd's. The apiserver's memory blowup is driven by the
// size of the single collection being relisted (the watch cache holds one full
// decoded copy per resource), not by the total database size. A per-collection
// bound therefore targets the actual failure mode more precisely; a cluster
// with 50 small collections is not a risk, whereas one 6 GiB collection is,
// and a global quota cannot distinguish those.
var maxCollectionBytes = envInt64("KUBE_APISERVER_TIKV_MAX_COLLECTION_BYTES", listMaxBytes)

// collectionSize is an approximate, self-correcting accounting of the total
// stored bytes in ONE resource collection (each store serves exactly one
// GroupResource, so a single counter per store suffices).
//
// Accuracy model -- read this before trusting the number:
//
//   - It is a GUARD, not an accountant. It is maintained per apiserver replica
//     with no cross-replica coordination, so two replicas each see only their
//     own writes plus whatever a full LIST last told them.
//   - It is corrected exactly on every completed UNPAGINATED LIST, which is
//     precisely the watch-cache relist that happens on every apiserver start
//     and on every watch-cache reset. That resync is authoritative because the
//     scan visited the whole collection and summed the real stored bytes.
//   - Between resyncs it drifts by whatever other replicas wrote.
//
// The drift direction is safe: an unseeded or stale counter is a LOWER bound,
// so the guard can only ever admit a write it should have rejected -- it can
// never reject a write it should have admitted. Under-blocking briefly is
// acceptable; falsely wedging a healthy cluster is not.
type collectionSize struct {
	approx atomic.Int64
	seeded atomic.Bool
}

// resync overwrites the estimate with an authoritative measurement taken by a
// completed full scan.
func (c *collectionSize) resync(totalBytes int64) {
	c.approx.Store(totalBytes)
	c.seeded.Store(true)
}

// add applies a signed delta from a single committed write or delete.  The
// counter is clamped at zero: drift must never make it negative, or a
// subsequent quota check would be meaninglessly permissive.
func (c *collectionSize) add(delta int64) {
	if n := c.approx.Add(delta); n < 0 {
		c.approx.Store(0)
	}
}

func (c *collectionSize) get() int64 { return c.approx.Load() }

// admitWrite rejects a write that would push this collection past
// maxCollectionBytes.
//
// incoming is the stored size of the value about to be written; replacing is
// the stored size of the value it overwrites (0 for a create), so an update
// that does not grow the object is always admitted even at quota.  That
// matters: at quota a cluster must still be able to shrink or rewrite objects
// in place, otherwise the operator has no way out except deleting data
// blind.
func (s *store) admitWrite(key string, incoming, replacing int64) error {
	if maxCollectionBytes <= 0 {
		return nil
	}
	projected := s.size.get() - replacing + incoming
	if projected <= maxCollectionBytes {
		return nil
	}
	// Never block a write that does not increase the collection's size, so
	// shrinking edits remain possible once the quota is hit.
	if incoming <= replacing {
		return nil
	}
	metrics.RecordCollectionQuotaRejection(s.groupResource)
	klog.V(2).InfoS("tikv: rejecting write, collection quota exceeded",
		"resource", s.groupResource.String(), "key", key,
		"approxBytes", s.size.get(), "incomingBytes", incoming,
		"replacingBytes", replacing, "quotaBytes", maxCollectionBytes)
	return apierrors.NewRequestEntityTooLargeError(fmt.Sprintf(
		"collection %s is approximately %d bytes and this write would exceed the %d byte "+
			"per-collection storage quota (analogous to etcd's --quota-backend-bytes NOSPACE alarm); "+
			"delete objects from this collection, or raise KUBE_APISERVER_TIKV_MAX_COLLECTION_BYTES "+
			"after confirming the apiserver has memory headroom for a full relist of the larger collection",
		s.groupResource.String(), s.size.get(), maxCollectionBytes))
}

// noteWrite records a committed write, and publishes the running estimate so
// keyspace growth is observable/alertable BEFORE it turns into an OOM.
func (s *store) noteWrite(delta int64) {
	s.size.add(delta)
	metrics.UpdateResourceTotalBytes(s.groupResource, s.size.get())
}
