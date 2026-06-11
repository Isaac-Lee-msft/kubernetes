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
	"encoding/binary"
	"sync"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"k8s.io/klog/v2"
)

const (
	// ttlMetaPrefix is prepended to the original key to form the corresponding
	// TTL metadata key.  For example, key "/registry/events/default/e1" has
	// TTL metadata at "/registry/__ttl__/events/default/e1".
	// The double underscore makes accidental collisions with real objects extremely
	// unlikely, and the prefix sorts together so range scans are efficient.
	ttlMetaPrefixSuffix = "__ttl__/"
)

// ttlManager tracks per-object TTLs by storing a small byte-encoded expiration
// timestamp alongside the normal object data in TiKV.  A background goroutine
// periodically sweeps expired entries and deletes them.
//
// TiKV does not have a native lease/TTL primitive, so this is a software
// emulation.  The granularity of expiration is bounded by reaperInterval.
type ttlManager struct {
	client         *txnkv.Client
	pathPrefix     string // e.g. "/registry/"
	reaperInterval time.Duration
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

func newTTLManager(client *txnkv.Client, pathPrefix string, reaperInterval time.Duration) *ttlManager {
	m := &ttlManager{
		client:         client,
		pathPrefix:     pathPrefix,
		reaperInterval: reaperInterval,
		stopCh:         make(chan struct{}),
	}
	m.wg.Add(1)
	go m.run()
	return m
}

// ttlKey returns the metadata key for a given object key.
func (m *ttlManager) ttlKey(objectKey []byte) []byte {
	// Insert "__ttl__/" immediately after pathPrefix.
	// e.g. "/registry/events/ns/name" → "/registry/__ttl__/events/ns/name"
	prefix := []byte(m.pathPrefix)
	suffix := objectKey[len(prefix):]
	key := make([]byte, 0, len(prefix)+len(ttlMetaPrefixSuffix)+len(suffix))
	key = append(key, prefix...)
	key = append(key, []byte(ttlMetaPrefixSuffix)...)
	key = append(key, suffix...)
	return key
}

// ttlScanPrefix returns the range prefix used to scan all TTL metadata keys.
func (m *ttlManager) ttlScanPrefix() []byte {
	return append([]byte(m.pathPrefix), []byte(ttlMetaPrefixSuffix)...)
}

// encodeTTLValue encodes an expiration timestamp as a big-endian uint64
// (Unix seconds) for storage as a TiKV value.
func encodeTTLValue(expireAt time.Time) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(expireAt.Unix()))
	return buf
}

// decodeTTLValue decodes a TTL value written by encodeTTLValue.
func decodeTTLValue(data []byte) (time.Time, bool) {
	if len(data) != 8 {
		return time.Time{}, false
	}
	unixSec := int64(binary.BigEndian.Uint64(data))
	return time.Unix(unixSec, 0), true
}

// SetTTL writes a TTL metadata entry for objectKey into the provided
// transaction.  The transaction must be committed by the caller.
func (m *ttlManager) SetTTL(txn *tikv.KVTxn, objectKey []byte, ttlSeconds uint64) error {
	expireAt := time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	return txn.Set(m.ttlKey(objectKey), encodeTTLValue(expireAt))
}

// ClearTTL removes the TTL metadata entry for objectKey from the transaction.
// It is a no-op if no TTL entry exists.
func (m *ttlManager) ClearTTL(txn *tikv.KVTxn, objectKey []byte) error {
	return txn.Delete(m.ttlKey(objectKey))
}

func (m *ttlManager) run() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.reap()
		case <-m.stopCh:
			return
		}
	}
}

func (m *ttlManager) reap() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	scanPrefix := m.ttlScanPrefix()
	// End key for the scan: increment the last byte of the prefix.
	scanEnd := prefixEnd(scanPrefix)

	now := time.Now()

	// Scan all TTL metadata keys.
	txnRead, err := m.client.Begin()
	if err != nil {
		klog.V(4).Infof("tikv ttlManager: begin scan txn failed: %v", err)
		return
	}
	iter, err := txnRead.Iter(scanPrefix, scanEnd)
	if err != nil {
		_ = txnRead.Rollback()
		klog.V(4).Infof("tikv ttlManager: scan iter failed: %v", err)
		return
	}

	type expired struct {
		ttlKey    []byte
		objectKey []byte
	}
	var toDelete []expired

	for iter.Valid() {
		ttlK := cloneBytes(iter.Key())
		expireAt, ok := decodeTTLValue(iter.Value())
		if ok && now.After(expireAt) {
			// Reconstruct object key from ttl key.
			// ttl key = pathPrefix + "__ttl__/" + objectSuffix
			// object key = pathPrefix + objectSuffix
			metaPrefix := []byte(m.pathPrefix + ttlMetaPrefixSuffix)
			objSuffix := ttlK[len(metaPrefix):]
			objKey := append([]byte(m.pathPrefix), objSuffix...)
			toDelete = append(toDelete, expired{ttlKey: ttlK, objectKey: objKey})
		}
		if err := iter.Next(); err != nil {
			break
		}
	}
	iter.Close()
	_ = txnRead.Rollback()

	for _, e := range toDelete {
		if err := m.deleteExpiredObject(ctx, e.ttlKey, e.objectKey); err != nil {
			klog.V(4).Infof("tikv ttlManager: failed to delete expired object %s: %v", e.objectKey, err)
		}
	}
}

func (m *ttlManager) deleteExpiredObject(ctx context.Context, ttlKey, objectKey []byte) error {
	for attempt := 0; attempt < 5; attempt++ {
		txn, err := m.client.Begin()
		if err != nil {
			return err
		}

		// Verify TTL entry still exists and is still expired (may have been
		// refreshed by an update).
		ttlVal, err := txn.Get(ctx, ttlKey)
		if tikverr.IsErrNotFound(err) {
			_ = txn.Rollback()
			return nil // already cleaned
		}
		if err != nil {
			_ = txn.Rollback()
			return err
		}
		expireAt, ok := decodeTTLValue(ttlVal)
		if !ok || time.Now().Before(expireAt) {
			_ = txn.Rollback()
			return nil // TTL was refreshed concurrently
		}

		if err := txn.Delete(objectKey); err != nil {
			_ = txn.Rollback()
			return err
		}
		if err := txn.Delete(ttlKey); err != nil {
			_ = txn.Rollback()
			return err
		}
		err = txn.Commit(ctx)
		if tikverr.IsErrWriteConflict(err) {
			continue // retry
		}
		return err
	}
	return nil
}

// Stop shuts down the background reaper.
func (m *ttlManager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}
