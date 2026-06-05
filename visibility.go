// Copyright 2024 dtq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package dtq — Visibility Timeout implementation.
//
// When a worker picks up a task, it sets a Redis key "asynq:visibility:{task_id}"
// with a TTL (default 30s). The worker renews this key every 10 seconds (heartbeat).
// The recoverer periodically scans for active tasks whose visibility key has
// expired and routes them back to the pending queue for reprocessing.

package dtq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// defaultVisibilityTimeout is the default TTL for visibility keys.
	defaultVisibilityTimeout = 30 * time.Second

	// visibilityRenewalInterval is how often the heartbeat renews the visibility key.
	visibilityRenewalInterval = 10 * time.Second

	// visibilityKeyPrefix is the Redis key prefix for visibility keys.
	visibilityKeyPrefix = "asynq:visibility:"
)

// VisibilityTracker manages per-task visibility keys in Redis.
// It allows workers to claim a task and renew the claim periodically.
type VisibilityTracker struct {
	client  redis.UniversalClient
	timeout time.Duration

	mu      sync.Mutex
	active  map[string]context.CancelFunc // taskID → cancel func for renewal goroutine
}

// NewVisibilityTracker creates a new VisibilityTracker.
func NewVisibilityTracker(client redis.UniversalClient, timeout time.Duration) *VisibilityTracker {
	if timeout <= 0 {
		timeout = defaultVisibilityTimeout
	}
	return &VisibilityTracker{
		client:  client,
		timeout: timeout,
		active:  make(map[string]context.CancelFunc),
	}
}

// Claim sets the visibility key for taskID and starts a heartbeat goroutine
// that renews the key until Release is called.
func (vt *VisibilityTracker) Claim(taskID string) error {
	key := visibilityKeyPrefix + taskID
	ctx := context.Background()
	if err := vt.client.Set(ctx, key, "1", vt.timeout).Err(); err != nil {
		return fmt.Errorf("visibility: failed to set key for task %s: %w", taskID, err)
	}
	// Start heartbeat renewal goroutine.
	renewCtx, cancel := context.WithCancel(context.Background())
	vt.mu.Lock()
	vt.active[taskID] = cancel
	vt.mu.Unlock()

	go vt.renew(renewCtx, taskID)
	return nil
}

// renew periodically extends the TTL of the visibility key until ctx is cancelled.
func (vt *VisibilityTracker) renew(ctx context.Context, taskID string) {
	ticker := time.NewTicker(visibilityRenewalInterval)
	defer ticker.Stop()
	key := visibilityKeyPrefix + taskID
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := vt.client.Expire(ctx, key, vt.timeout).Err(); err != nil {
				// Context may be cancelled; ignore the error.
				return
			}
		}
	}
}

// Release stops the heartbeat and deletes the visibility key for taskID.
func (vt *VisibilityTracker) Release(taskID string) {
	vt.mu.Lock()
	cancel, ok := vt.active[taskID]
	if ok {
		delete(vt.active, taskID)
	}
	vt.mu.Unlock()

	if ok {
		cancel()
	}
	// Delete the visibility key.
	vt.client.Del(context.Background(), visibilityKeyPrefix+taskID)
}

// IsVisible reports whether the visibility key for taskID still exists.
// A false result means the key has expired (worker crashed).
func (vt *VisibilityTracker) IsVisible(taskID string) (bool, error) {
	key := visibilityKeyPrefix + taskID
	exists, err := vt.client.Exists(context.Background(), key).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}
