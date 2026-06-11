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

package factory

import (
	"context"
	"fmt"
	"sync"
	"time"

	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	etcd3metrics "k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	tikvstorage "k8s.io/apiserver/pkg/storage/tikv"
	tikvmetrics "k8s.io/apiserver/pkg/storage/tikv/metrics"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
)

const (
	// defaultTiKVPollInterval is the default watch-poll interval for TiKV.
	defaultTiKVPollInterval = 250 * time.Millisecond
	// defaultTiKVReaperInterval is the default TTL-reaper scan interval.
	defaultTiKVReaperInterval = 10 * time.Second
	// tikvGCServicePrefix is the PD service name prefix for GC safepoint registration.
	tikvGCServicePrefix = "kube-apiserver-tikv"
)

func init() {
	tikvmetrics.Register()
}

// newTiKVStorage creates a storage.Interface backed by TiKV.
func newTiKVStorage(
	c storagebackend.ConfigForResource,
	newFunc, newListFunc func() runtime.Object,
	resourcePrefix string,
) (storage.Interface, DestroyFunc, error) {
	if len(c.Transport.ServerList) == 0 {
		return nil, nil, fmt.Errorf("tikv: no PD endpoints provided (use --etcd-servers to specify PD addresses)")
	}

	client, err := newTiKVClient(c)
	if err != nil {
		return nil, nil, fmt.Errorf("tikv: failed to create client: %w", err)
	}

	transformer := c.Transformer
	if transformer == nil {
		transformer = identity.NewEncryptCheckTransformer()
	}

	comp := tikvstorage.NewCompactor(
		client,
		tikvGCServicePrefix,
		c.EventsHistoryWindow,
		c.CompactionInterval,
	)

	store := tikvstorage.New(
		client,
		comp,
		c.Codec,
		newFunc,
		newListFunc,
		c.Prefix,
		resourcePrefix,
		c.GroupResource,
		transformer,
		defaultTiKVPollInterval,
		defaultTiKVReaperInterval,
	)

	var once sync.Once
	destroyFunc := func() {
		once.Do(func() {
			comp.Stop()
			store.Close()
			_ = client.Close()
		})
	}
	return store, destroyFunc, nil
}

// tikvGlobalConfigMu serialises mutations of the tikv/client-go global config.
// The client-go v2 library reads TLS settings from the process-wide
// config.GetGlobalConfig() value; constructing multiple clients concurrently
// would race on that global.
var tikvGlobalConfigMu sync.Mutex

// newTiKVClient creates a TiKV client from the storage backend config.
// PD endpoints are taken from c.Transport.ServerList (the same flag as --etcd-servers).
//
// TLS settings are applied by setting the tikv/client-go process-global config
// because that library does not accept per-client security options in v2.0.x.
//
// IMPORTANT: the global Security config is set *permanently* and is deliberately
// NOT restored after NewClient returns.  client-go reads the global config
// lazily on background code paths that run long after construction — most
// notably the region cache's store health-check dialer
// (internal/locate/region_cache.go createKVHealthClient), which re-reads
// config.GetGlobalConfig().Security every time it probes a TiKV store.  If the
// TLS settings were reverted (as a deferred restore would do), those background
// health-check connections would be dialed in plaintext against TiKV's
// TLS-only KV port, fail the handshake ("wrong version number"), and cause the
// region cache to mark healthy stores — including region leaders — as
// unreachable.  That manifests as a flood of "not leader for region" retries
// and multi-second latency on every storage operation.  All clients in this
// process talk to the same PD/TiKV cluster with the same identity, so a single
// permanent global Security value is correct.
func newTiKVClientCommon(serverList []string, caFile, certFile, keyFile string) (*txnkv.Client, error) {
	if caFile == "" && certFile == "" {
		return txnkv.NewClient(serverList)
	}
	security := tikvconfig.NewSecurity(caFile, certFile, keyFile, []string{})
	tikvGlobalConfigMu.Lock()
	defer tikvGlobalConfigMu.Unlock()
	tikvconfig.UpdateGlobal(func(conf *tikvconfig.Config) {
		conf.Security = security
	})
	return txnkv.NewClient(serverList)
}

func newTiKVClient(c storagebackend.ConfigForResource) (*txnkv.Client, error) {
	return newTiKVClientCommon(
		c.Transport.ServerList,
		c.Transport.TrustedCAFile,
		c.Transport.CertFile,
		c.Transport.KeyFile,
	)
}

func newTiKVClientFromConfig(c storagebackend.Config) (*txnkv.Client, error) {
	return newTiKVClientCommon(
		c.Transport.ServerList,
		c.Transport.TrustedCAFile,
		c.Transport.CertFile,
		c.Transport.KeyFile,
	)
}

// tikvProber implements both the Prober and metrics.Monitor interfaces for TiKV.
type tikvProber struct {
	client *txnkv.Client
}

func newTiKVProber(c storagebackend.Config) (*tikvProber, error) {
	if len(c.Transport.ServerList) == 0 {
		return nil, fmt.Errorf("tikv: no PD endpoints provided")
	}
	client, err := newTiKVClientFromConfig(c)
	if err != nil {
		return nil, fmt.Errorf("tikv: prober client creation failed: %w", err)
	}
	return &tikvProber{client: client}, nil
}

// Probe implements Prober.
func (p *tikvProber) Probe(ctx context.Context) error {
	_, err := p.client.GetTimestamp(ctx)
	return err
}

// Monitor implements etcd3metrics.Monitor by returning a best-effort storage size.
func (p *tikvProber) Monitor(ctx context.Context) (etcd3metrics.StorageMetrics, error) {
	// TiKV does not expose a simple total-size API in client-go v2.
	// Return a zero-value result; a full implementation would aggregate region
	// statistics from PD.
	return etcd3metrics.StorageMetrics{}, nil
}

// Close implements Prober and etcd3metrics.Monitor.
func (p *tikvProber) Close() error {
	return p.client.Close()
}

// newTiKVHealthCheck returns a function that checks TiKV/PD health.
func newTiKVHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	if len(c.Transport.ServerList) == 0 {
		return nil, fmt.Errorf("tikv: no PD endpoints for health check")
	}
	client, err := newTiKVClientFromConfig(c)
	if err != nil {
		return nil, fmt.Errorf("tikv: health check client creation failed: %w", err)
	}
	go func() {
		<-stopCh
		_ = client.Close()
	}()
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), c.HealthcheckTimeout)
		defer cancel()
		_, err := client.GetTimestamp(ctx)
		return err
	}, nil
}

// newTiKVReadyCheck returns a readiness check function for TiKV.
func newTiKVReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	if len(c.Transport.ServerList) == 0 {
		return nil, fmt.Errorf("tikv: no PD endpoints for readiness check")
	}
	client, err := newTiKVClientFromConfig(c)
	if err != nil {
		return nil, fmt.Errorf("tikv: readiness check client creation failed: %w", err)
	}
	go func() {
		<-stopCh
		_ = client.Close()
	}()
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), c.ReadycheckTimeout)
		defer cancel()
		txn, err := client.Begin()
		if err != nil {
			return err
		}
		_, _ = txn.Get(ctx, []byte("/readyz"))
		return txn.Rollback()
	}, nil
}
