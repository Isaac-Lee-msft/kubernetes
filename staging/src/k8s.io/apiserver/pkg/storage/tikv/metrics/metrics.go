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

// Package metrics provides Prometheus metrics for the TiKV storage backend.
package metrics

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	tikvRequestLatency = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "tikv_request_duration_seconds",
			Help:           "TiKV request latency in seconds for each operation and object type.",
			Buckets:        []float64{0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3, 4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"operation", "group", "resource"},
	)

	tikvRequestCounts = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "tikv_requests_total",
			Help:           "TiKV request counts for each operation and object type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"operation", "group", "resource"},
	)

	tikvObjectCounts = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "tikv_object_counts",
			Help:           "Number of stored objects at the time of last check split by kind.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"resource"},
	)

	tikvDbTotalSize = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "tikv_db_total_size_in_bytes",
			Help:           "Total size of the storage database file physically allocated in bytes.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"endpoint"},
	)

	tikvWatchEventsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "tikv_watch_events_total",
			Help:           "Number of events emitted from tikv watcher, per type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"type"},
	)

	tikvTxnConflictsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "tikv_txn_conflicts_total",
			Help:           "Total number of transaction write conflicts (optimistic lock retries), per resource.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvListReturnedObjects = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "tikv_list_returned_objects",
			Help:           "Number of objects returned to the caller for each LIST, per resource.",
			Buckets:        []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvListScannedBytes = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "tikv_list_scanned_bytes",
			Help:           "Total value bytes scanned from TiKV for each LIST, per resource.",
			Buckets:        compbasemetrics.ExponentialBuckets(1024, 4, 12),
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvListPagesTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "tikv_list_pages_total",
			Help:           "Total number of bounded scan pages fetched from TiKV while serving LISTs, per resource.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvGuaranteedUpdateRetriesTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "tikv_guaranteed_update_retries_total",
			Help:           "Total number of GuaranteedUpdate retries (conflict or stale re-read), per resource.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvResourceTotalBytes = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "tikv_resource_total_bytes",
			Help:           "Approximate total value bytes stored for a resource, observed during the last full (unpaginated) LIST. Tracks keyspace growth that precedes an apiserver list OOM.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvWatchPollDuration = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "tikv_watch_poll_duration_seconds",
			Help:           "Duration of a single MVCC watch poll (scan + diff + emit), per resource.",
			Buckets:        []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvWatchTrackedKeys = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "tikv_watch_tracked_keys",
			Help:           "Number of keys whose change fingerprints a watch currently retains, per resource. Proxy for per-watch steady-state memory.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvWatchReconstructReadsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "tikv_watch_reconstruct_reads_total",
			Help:           "Total historical single-key reads issued to reconstruct prior objects for Deleted/predicate-transition watch events, per resource.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	tikvGCSafepointTimestamp = compbasemetrics.NewGauge(
		&compbasemetrics.GaugeOpts{
			Name:           "tikv_gc_safepoint_timestamp_seconds",
			Help:           "The current GC safepoint timestamp maintained by this apiserver (unix seconds).",
			StabilityLevel: compbasemetrics.ALPHA,
		},
	)

	tikvWatchOpenTotal = compbasemetrics.NewGauge(
		&compbasemetrics.GaugeOpts{
			Name:           "tikv_watch_open_total",
			Help:           "Number of currently active TiKV watch streams.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
	)
)

var registerOnce sync.Once

// Register registers all TiKV metrics with the legacy registry. Safe to call multiple times.
func Register() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(tikvRequestLatency)
		legacyregistry.MustRegister(tikvRequestCounts)
		legacyregistry.MustRegister(tikvObjectCounts)
		legacyregistry.MustRegister(tikvDbTotalSize)
		legacyregistry.MustRegister(tikvWatchEventsTotal)
		legacyregistry.MustRegister(tikvTxnConflictsTotal)
		legacyregistry.MustRegister(tikvListReturnedObjects)
		legacyregistry.MustRegister(tikvListScannedBytes)
		legacyregistry.MustRegister(tikvListPagesTotal)
		legacyregistry.MustRegister(tikvGuaranteedUpdateRetriesTotal)
		legacyregistry.MustRegister(tikvResourceTotalBytes)
		legacyregistry.MustRegister(tikvWatchPollDuration)
		legacyregistry.MustRegister(tikvWatchTrackedKeys)
		legacyregistry.MustRegister(tikvWatchReconstructReadsTotal)
		legacyregistry.MustRegister(tikvGCSafepointTimestamp)
		legacyregistry.MustRegister(tikvWatchOpenTotal)
	})
}

// RecordRequest records latency and increments the count for a TiKV operation.
func RecordRequest(operation string, groupResource schema.GroupResource, err error, startTime time.Time) {
	tikvRequestLatency.WithLabelValues(operation, groupResource.Group, groupResource.Resource).Observe(time.Since(startTime).Seconds())
	tikvRequestCounts.WithLabelValues(operation, groupResource.Group, groupResource.Resource).Inc()
}

// UpdateObjectCount updates the object count gauge for a resource.
func UpdateObjectCount(resourcePrefix string, count int64) {
	tikvObjectCounts.WithLabelValues(resourcePrefix).Set(float64(count))
}

// UpdateDbSize updates the database size gauge for a store endpoint.
func UpdateDbSize(endpoint string, size int64) {
	tikvDbTotalSize.WithLabelValues(endpoint).Set(float64(size))
}

// RecordWatchEvent increments the watch event counter for the given event type.
func RecordWatchEvent(eventType string) {
	tikvWatchEventsTotal.WithLabelValues(eventType).Inc()
}

// RecordTxnConflict increments the transaction conflict counter for a resource.
func RecordTxnConflict(groupResource schema.GroupResource) {
	tikvTxnConflictsTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// RecordList records cost metrics for a single LIST: the number of objects
// returned to the caller, the total value bytes scanned from TiKV, and the
// number of bounded scan pages that were fetched.
func RecordList(groupResource schema.GroupResource, returnedObjects int, scannedBytes int64, pages int) {
	tikvListReturnedObjects.WithLabelValues(groupResource.Group, groupResource.Resource).Observe(float64(returnedObjects))
	tikvListScannedBytes.WithLabelValues(groupResource.Group, groupResource.Resource).Observe(float64(scannedBytes))
	if pages > 0 {
		tikvListPagesTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Add(float64(pages))
	}
}

// RecordGuaranteedUpdateRetry increments the GuaranteedUpdate retry counter for a resource.
func RecordGuaranteedUpdateRetry(groupResource schema.GroupResource) {
	tikvGuaranteedUpdateRetriesTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// UpdateResourceTotalBytes sets the approximate total stored value bytes for a
// resource, as observed during a full (unpaginated) LIST scan.
func UpdateResourceTotalBytes(groupResource schema.GroupResource, bytes int64) {
	tikvResourceTotalBytes.WithLabelValues(groupResource.Group, groupResource.Resource).Set(float64(bytes))
}

// RecordWatchPoll records the duration of a single MVCC watch poll and the
// number of keys currently tracked by that watch (proxy for per-watch memory).
func RecordWatchPoll(groupResource schema.GroupResource, d time.Duration, trackedKeys int) {
	tikvWatchPollDuration.WithLabelValues(groupResource.Group, groupResource.Resource).Observe(d.Seconds())
	tikvWatchTrackedKeys.WithLabelValues(groupResource.Group, groupResource.Resource).Set(float64(trackedKeys))
}

// RecordWatchReconstructRead increments the historical reconstruct-read counter
// for a resource (issued for Deleted/predicate-transition events).
func RecordWatchReconstructRead(groupResource schema.GroupResource) {
	tikvWatchReconstructReadsTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// UpdateGCSafepoint sets the GC safepoint gauge (in unix seconds).
func UpdateGCSafepoint(ts float64) {
	tikvGCSafepointTimestamp.Set(ts)
}

// IncWatchOpen increments the open watches gauge.
func IncWatchOpen() {
	tikvWatchOpenTotal.Inc()
}

// DecWatchOpen decrements the open watches gauge.
func DecWatchOpen() {
	tikvWatchOpenTotal.Dec()
}
