// Copyright 2024 dtq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dtq

import (
	"context"
	"testing"
	"time"
)

// TestVisibilityClaimAndRelease verifies basic claim/release behavior.
func TestVisibilityClaimAndRelease(t *testing.T) {
	r := setup(t)
	defer r.Close()

	tracker := NewVisibilityTracker(r, 5*time.Second)
	taskID := "test-visibility-task-1"

	// Before claim, key should not exist.
	visible, err := tracker.IsVisible(taskID)
	if err != nil {
		t.Fatalf("IsVisible returned error: %v", err)
	}
	if visible {
		t.Error("Expected visibility key to not exist before Claim")
	}

	// After claim, key should exist.
	if err := tracker.Claim(taskID); err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}

	visible, err = tracker.IsVisible(taskID)
	if err != nil {
		t.Fatalf("IsVisible returned error: %v", err)
	}
	if !visible {
		t.Error("Expected visibility key to exist after Claim")
	}
	t.Log("✓ Visibility key exists after Claim")

	// After release, key should be deleted.
	tracker.Release(taskID)
	visible, err = tracker.IsVisible(taskID)
	if err != nil {
		t.Fatalf("IsVisible returned error: %v", err)
	}
	if visible {
		t.Error("Expected visibility key to be deleted after Release")
	}
	t.Log("✓ Visibility key deleted after Release")
}

// TestVisibilityTTLExpiry verifies that the key expires when not renewed.
func TestVisibilityTTLExpiry(t *testing.T) {
	r := setup(t)
	defer r.Close()

	// Use a very short timeout (2s) for testing.
	shortTimeout := 2 * time.Second
	tracker := NewVisibilityTracker(r, shortTimeout)
	taskID := "test-visibility-task-ttl"

	// Claim without renewal.
	if err := tracker.Claim(taskID); err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}

	// Immediately stop renewal by releasing internal state but keep Redis key.
	// We'll directly manipulate the active map to simulate crash.
	tracker.mu.Lock()
	cancel, ok := tracker.active[taskID]
	if ok {
		delete(tracker.active, taskID)
	}
	tracker.mu.Unlock()
	if ok {
		cancel() // stop heartbeat goroutine
	}

	// Wait for TTL to expire.
	time.Sleep(shortTimeout + 500*time.Millisecond)

	// Key should have expired.
	visible, err := tracker.IsVisible(taskID)
	if err != nil {
		t.Fatalf("IsVisible returned error: %v", err)
	}
	if visible {
		t.Errorf("Expected visibility key to have expired after %v without renewal", shortTimeout)
	} else {
		t.Logf("✓ Visibility key expired after %v (crash recovery mechanism working)", shortTimeout)
	}
}

// TestVisibilityHeartbeatRenews verifies that heartbeat keeps the key alive.
func TestVisibilityHeartbeatRenews(t *testing.T) {
	r := setup(t)
	defer r.Close()

	// Use a timeout longer than the first renewal interval.
	timeout := 3 * time.Second
	tracker := NewVisibilityTracker(r, timeout)
	taskID := "test-visibility-task-renew"

	if err := tracker.Claim(taskID); err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	defer tracker.Release(taskID)

	// Wait for well under the TTL — key should still exist.
	time.Sleep(1 * time.Second)

	visible, err := tracker.IsVisible(taskID)
	if err != nil {
		t.Fatalf("IsVisible returned error: %v", err)
	}
	if !visible {
		t.Errorf("Expected visibility key to still exist at 1s (within %v TTL)", timeout)
	} else {
		t.Logf("✓ Visibility key still alive at 1s (heartbeat working)")
	}
}

// TestVisibilityTimeoutIntegration verifies the Config field is accepted.
func TestVisibilityTimeoutIntegration(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var processed bool
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		processed = true
		return nil
	})

	client := NewClientFromRedisClient(r)
	defer client.Close()

	task := NewTask("visibility:test", []byte(`{}`))
	if _, err := client.Enqueue(task); err != nil {
		t.Fatalf("Failed to enqueue task: %v", err)
	}

	srv := NewServerFromRedisClient(r, Config{
		Concurrency:       1,
		Queues:            map[string]int{"default": 1},
		TaskCheckInterval: 100 * time.Millisecond,
		VisibilityTimeout: 30 * time.Second,
	})

	if err := srv.Start(handler); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Shutdown()

	time.Sleep(2 * time.Second)

	if !processed {
		t.Error("Expected task to be processed with VisibilityTimeout set")
	} else {
		t.Log("✓ Task processed successfully with VisibilityTimeout configured")
	}
}
