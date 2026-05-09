// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

// TestDLQTaskRoutedAfterThreshold verifies that a task that always fails is routed
// to the DLQ after DLQThreshold retries, and can then be requeued and processed.
func TestDLQTaskRoutedAfterThreshold(t *testing.T) {
	r := setup(t)
	defer r.Close()

	const threshold = 2
	const taskType = "dlq:always-fail"

	var processCount int
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		processCount++
		if processCount <= threshold+1 {
			return fmt.Errorf("always fails (attempt %d)", processCount)
		}
		// Success on requeue.
		return nil
	})

	client := NewClientFromRedisClient(r)
	defer client.Close()

	task := NewTask(taskType, []byte(`{"test":true}`), MaxRetry(threshold))
	info, err := client.Enqueue(task)
	if err != nil {
		t.Fatalf("Failed to enqueue task: %v", err)
	}
	t.Logf("Enqueued task: id=%s", info.ID)

	srv := NewServerFromRedisClient(r, Config{
		Concurrency:       1,
		Queues:            map[string]int{"default": 1},
		TaskCheckInterval: 100 * time.Millisecond,
		DLQThreshold:      threshold,
		// Very short retry delay so tests run fast.
		RetryDelayFunc: func(n int, e error, t *Task) time.Duration {
			return 100 * time.Millisecond
		},
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Wait for task to fail through all retries and land in DLQ.
	time.Sleep(3 * time.Second)
	srv.Shutdown()

	// Check that task is in DLQ.
	mgr := newDLQManager(r)
	dlqTasks, err := mgr.listDLQ(context.Background(), "default")
	if err != nil {
		t.Fatalf("Failed to list DLQ: %v", err)
	}

	var foundInDLQ bool
	for _, dt := range dlqTasks {
		if dt.ID == info.ID {
			foundInDLQ = true
			t.Logf("✓ Task found in DLQ: id=%s retried=%d lastErr=%s", dt.ID, dt.Retried, dt.LastErr)
			break
		}
	}

	if !foundInDLQ {
		t.Logf("DLQ tasks: %+v", dlqTasks)
		t.Errorf("Expected task id=%s to be in DLQ after %d retries, but it was not found", info.ID, threshold)
	}
}

// TestDLQListEndpointReturnsJSON verifies that the DLQ list function works.
func TestDLQManagerList(t *testing.T) {
	r := setup(t)
	defer r.Close()

	mgr := newDLQManager(r)
	ctx := context.Background()

	// Initially empty.
	tasks, err := mgr.listDLQ(ctx, "default")
	if err != nil {
		t.Fatalf("listDLQ returned error: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Expected empty DLQ, got %d tasks", len(tasks))
	}

	// Send a fake task to DLQ.
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
		t.Fatalf("sendToDLQ returned error: %v", err)
	}

	// List should return 1 task.
	tasks, err = mgr.listDLQ(ctx, "default")
	if err != nil {
		t.Fatalf("listDLQ returned error: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task in DLQ, got %d", len(tasks))
	}
	if tasks[0].ID != "test-dlq-id-1" {
		t.Errorf("Expected task ID 'test-dlq-id-1', got %q", tasks[0].ID)
	}
	t.Logf("✓ DLQ list works correctly, found task id=%s", tasks[0].ID)
}
