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
	"sync"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/txnkv"
	"k8s.io/apiserver/pkg/storage/tikv/metrics"
	"k8s.io/klog/v2"
)

const (
	// gcServiceID is the PD service name used when registering the GC safepoint.
	// Each apiserver registers under a unique ID derived from this prefix.
	gcServiceIDPrefix = "kube-apiserver-tikv"

	// gcSafepointTTL is how long (in seconds) the registered safepoint remains valid
	// after the last update.  It must be longer than gcInterval so that brief restarts
	// do not prematurely retract the safepoint.
	gcSafepointTTL = int64(600) // 10 minutes
)

// Compactor abstracts the GC safepoint management so the factory can stop it
// without depending on the concrete type.
type Compactor interface {
	CompactRevision() int64
	Stop()
}

// compactor manages the TiKV GC safepoint on behalf of the kube-apiserver. It
// periodically advances the safepoint to (now - eventsHistoryWindow), ensuring
// that MVCC versions still needed by active watch streams are retained while
// older versions are eligible for GC.
type compactor struct {
	client              *txnkv.Client
	serviceID           string
	eventsHistoryWindow time.Duration
	interval            time.Duration
	stopCh              chan struct{}
	lastSafepoint       atomic.Uint64
	mu                  sync.RWMutex
	wg                  sync.WaitGroup
}

// NewCompactor creates and starts a Compactor.  The caller must call Stop() to
// release resources.  If interval is zero, compaction is disabled and a no-op
// Compactor is returned.
func NewCompactor(client *txnkv.Client, serviceID string, eventsHistoryWindow, interval time.Duration) Compactor {
	if interval == 0 {
		return noopCompactor{}
	}
	return newCompactor(client, serviceID, eventsHistoryWindow, interval)
}

// newCompactor creates and starts a compactor.
func newCompactor(client *txnkv.Client, serviceID string, eventsHistoryWindow, interval time.Duration) *compactor {
	c := &compactor{
		client:              client,
		serviceID:           serviceID,
		eventsHistoryWindow: eventsHistoryWindow,
		interval:            interval,
		stopCh:              make(chan struct{}),
	}
	c.wg.Add(1)
	go c.run()
	return c
}

func (c *compactor) run() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.advanceSafepoint()
		case <-c.stopCh:
			return
		}
	}
}

func (c *compactor) advanceSafepoint() {
	// Compute the new safepoint as the physical timestamp of (now - eventsHistoryWindow).
	// TiKV TSO encodes physical time (ms) in the high 46 bits; here we retain only the
	// physical-time portion and leave logical bits zero, which is conservative and safe.
	cutoff := time.Now().Add(-c.eventsHistoryWindow)
	// TSO format: physicalMs<<18 | logicalBits — set logical = 0.
	physMs := uint64(cutoff.UnixMilli())
	safepoint := physMs << 18

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.client.GetPDClient().UpdateServiceGCSafePoint(ctx, c.serviceID, gcSafepointTTL, safepoint)
	if err != nil {
		klog.V(4).Infof("tikv: failed to advance GC safepoint: %v", err)
		return
	}
	c.lastSafepoint.Store(safepoint)
	metrics.UpdateGCSafepoint(float64(cutoff.Unix()))
	klog.V(6).Infof("tikv: GC safepoint advanced to TSO %d (cutoff %s)", safepoint, cutoff.Format(time.RFC3339))
}

// CompactRevision returns the last successfully registered GC safepoint as a
// revision value comparable to storage.Interface.CompactRevision().
func (c *compactor) CompactRevision() int64 {
	return int64(c.lastSafepoint.Load())
}

// Stop shuts down the compactor goroutine and waits for it to exit.
func (c *compactor) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// noopCompactor is returned by NewCompactor when compaction is disabled
// (interval == 0).  It satisfies the Compactor interface with no-op implementations.
type noopCompactor struct{}

func (noopCompactor) CompactRevision() int64 { return 0 }
func (noopCompactor) Stop()                  {}
