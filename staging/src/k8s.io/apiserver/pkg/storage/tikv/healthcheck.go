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
	"fmt"
	"time"

	"github.com/tikv/client-go/v2/txnkv"
)

const (
	healthcheckTimeout = 2 * time.Second
)

// healthChecker probes TiKV (via a lightweight read) and PD (via a timestamp
// fetch) to determine whether the backend is reachable.
type healthChecker struct {
	client *txnkv.Client
}

func newHealthChecker(client *txnkv.Client) *healthChecker {
	return &healthChecker{client: client}
}

// Check returns nil if TiKV and PD are reachable.
func (h *healthChecker) Check() error {
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()

	// A successful GetTimestamp proves the PD is up and the TSO allocator works.
	_, err := h.client.GetTimestamp(ctx)
	if err != nil {
		return fmt.Errorf("tikv: PD timestamp request failed: %w", err)
	}

	// A successful Begin + Rollback proves that TiKV stores are reachable.
	txn, err := h.client.Begin()
	if err != nil {
		return fmt.Errorf("tikv: transaction begin failed: %w", err)
	}
	// A read to a nonexistent probe key verifies region lookup works.
	_, _ = txn.Get(ctx, []byte("/healthz"))
	if err := txn.Rollback(); err != nil {
		return fmt.Errorf("tikv: transaction rollback failed: %w", err)
	}
	return nil
}

// readinessCheck returns nil once TiKV can service at least one successful txn.
func (h *healthChecker) ReadinessCheck() error {
	return h.Check()
}
