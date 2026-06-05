// Copyright 2024 dtq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dtq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestIdempotencyExactlyOnce verifies that when two tasks are enqueued with the
// same IdempotencyKey, the handler is called exactly once.
func TestIdempotencyExactlyOnce(t *testing.T) {
	// Skip if no Redis connection available.
	r := setup(t)
	defer r.Close()

	var callCount atomic.Int32

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		callCount.Add(1)
		return nil
	})

	const idemKey = "test-payment-charge-order-42"
	const taskType = "idempotency:test"

	// Enqueue the same task twice with the same idempotency key.
	client := NewClientFromRedisClient(r)
	defer client.Close()

	// Flush any previous idempotency keys for clean test.
	r.Del(context.Background(), idempotencyKeyPrefix+idemKey)

	task1 := NewTask(taskType, []byte(`{"order_id":42}`),
		IdempotencyKey(idemKey, 24*time.Hour))
	task2 := NewTask(taskType, []byte(`{"order_id":42}`),
		IdempotencyKey(idemKey, 24*time.Hour))

	info1, err := client.Enqueue(task1)
	if err != nil {
		t.Fatalf("Failed to enqueue task1: %v", err)
	}
	t.Logf("Enqueued task1: id=%s", info1.ID)

	info2, err := client.Enqueue(task2)
	if err != nil {
		t.Fatalf("Failed to enqueue task2: %v", err)
	}
	t.Logf("Enqueued task2: id=%s", info2.ID)

	// Start a server and process both tasks.
	srv := NewServerFromRedisClient(r, Config{
		Concurrency: 2,
		Queues:      map[string]int{"default": 1},
		// Use short check interval so tests run fast.
		TaskCheckInterval: 100 * time.Millisecond,
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Shutdown()

	// Wait long enough for both tasks to be processed.
	time.Sleep(2 * time.Second)

	// The handler must have been called exactly once.
	count := callCount.Load()
	if count != 1 {
		t.Errorf("Expected handler to be called exactly once, got %d", count)
	} else {
		t.Logf("✓ Handler called exactly once (idempotency working correctly)")
	}
}

// TestIdempotencyDifferentKeys verifies that tasks with different idempotency
// keys are both processed (no false deduplication).
func TestIdempotencyDifferentKeys(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var callCount atomic.Int32

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		callCount.Add(1)
		return nil
	})

	client := NewClientFromRedisClient(r)
	defer client.Close()

	// Clean up keys.
	r.Del(context.Background(), idempotencyKeyPrefix+"key-A", idempotencyKeyPrefix+"key-B")

	// Enqueue two tasks with different idempotency keys.
	task1 := NewTask("idempotency:test2", []byte(`{"id":1}`),
		IdempotencyKey("key-A", 24*time.Hour))
	task2 := NewTask("idempotency:test2", []byte(`{"id":2}`),
		IdempotencyKey("key-B", 24*time.Hour))

	if _, err := client.Enqueue(task1); err != nil {
		t.Fatalf("Failed to enqueue task1: %v", err)
	}
	if _, err := client.Enqueue(task2); err != nil {
		t.Fatalf("Failed to enqueue task2: %v", err)
	}

	srv := NewServerFromRedisClient(r, Config{
		Concurrency:       2,
		Queues:            map[string]int{"default": 1},
		TaskCheckInterval: 100 * time.Millisecond,
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Shutdown()

	time.Sleep(2 * time.Second)

	count := callCount.Load()
	if count != 2 {
		t.Errorf("Expected handler to be called twice (different keys), got %d", count)
	} else {
		t.Logf("✓ Both tasks processed (different idempotency keys, no false deduplication)")
	}
}
