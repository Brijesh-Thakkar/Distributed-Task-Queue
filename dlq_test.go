// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

// TestDLQTaskRoutedAfterThreshold verifies that a task is routed to the DLQ
// when it is retried at least DLQThreshold times.
//
// Strategy:
//   - DLQThreshold: 1  → route to DLQ once Retried >= 1
//   - MaxRetry(1)      → allows exactly 1 retry (Retried goes from 0 → 1)
//   - Attempt 0: Retried=0, returns error  → scheduled for retry
//   - Attempt 1: Retried=1, returns SkipRetry → exhaustion path:
//       msg.Retried(1) >= dlqThreshold(1) → task goes to DLQ
//   - RetryDelayFunc: 100ms for fast test
//   - forwarder runs every 5s; 10s wait covers ≥1 forwarder cycle
func TestDLQTaskRoutedAfterThreshold(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var callCount atomic.Int32

	client := NewClientFromRedisClient(r)
	defer client.Close()

	task := NewTask("dlq:route-test", []byte(`{}`), MaxRetry(1))
	info, err := client.Enqueue(task)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	t.Logf("enqueued task id=%s", info.ID)

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		n := callCount.Add(1)
		t.Logf("attempt %d for task %s", n, info.ID)
		if n >= 2 {
			// Second attempt: return SkipRetry so retries are immediately exhausted.
			// msg.Retried == 1 at this point, which is >= DLQThreshold(1) → DLQ.
			return fmt.Errorf("permanent: %w", SkipRetry)
		}
		return fmt.Errorf("transient failure (attempt %d)", n)
	})

	srv := NewServerFromRedisClient(r, Config{
		Concurrency:       1,
		Queues:            map[string]int{"default": 1},
		TaskCheckInterval: 50 * time.Millisecond,
		DLQThreshold:      1,
		RetryDelayFunc: func(n int, e error, t *Task) time.Duration {
			return 100 * time.Millisecond // very short so retry happens quickly
		},
	})
	if err := srv.Start(handler); err != nil {
		t.Fatalf("start server: %v", err)
	}

	// 10s: enough for both attempts + ≥1 forwarder cycle (runs every 5s).
	time.Sleep(10 * time.Second)
	srv.Shutdown()

	t.Logf("total call count: %d", callCount.Load())

	mgr := newDLQManager(r)
	dlqTasks, err := mgr.listDLQ(context.Background(), "default")
	if err != nil {
		t.Fatalf("listDLQ: %v", err)
	}

	var found bool
	for _, dt := range dlqTasks {
		if dt.ID == info.ID {
			found = true
			t.Logf("✓ task found in DLQ: id=%s retried=%d lastErr=%s", dt.ID, dt.Retried, dt.LastErr)
		}
	}
	if !found {
		t.Logf("calls=%d, dlq_size=%d", callCount.Load(), len(dlqTasks))
		t.Errorf("task id=%s not found in DLQ; check DLQ routing logic", info.ID)
	}
}

// TestDLQManagerList verifies low-level DLQ list/send operations.
func TestDLQManagerList(t *testing.T) {
	r := setup(t)
	defer r.Close()

	mgr := newDLQManager(r)
	ctx := context.Background()

	// Initially empty.
	tasks, err := mgr.listDLQ(ctx, "default")
	if err != nil {
		t.Fatalf("listDLQ: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("expected empty DLQ, got %d tasks", len(tasks))
	}

	// Insert a task directly into DLQ.
	fakeMsg := &base.TaskMessage{
		ID:           "test-dlq-id-1",
		Type:         "test:task",
		Payload:      []byte(`{}`),
		Queue:        "default",
		Retry:        3,
		Retried:      3,
		ErrorMsg:     "connection refused",
		LastFailedAt: time.Now().Unix(),
	}
	if err := mgr.sendToDLQ(ctx, "default", fakeMsg); err != nil {
		t.Fatalf("sendToDLQ: %v", err)
	}

	tasks, err = mgr.listDLQ(ctx, "default")
	if err != nil {
		t.Fatalf("listDLQ: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].ID != "test-dlq-id-1" {
		t.Errorf("expected id 'test-dlq-id-1', got %q", tasks[0].ID)
	}
	t.Logf("✓ DLQ list: found task id=%s", tasks[0].ID)
}
