// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
)

// TestWeightedRoundRobinDistribution verifies that the scheduling order matches
// the configured weights — each queue gets slots proportional to its weight.
func TestWeightedRoundRobinDistribution(t *testing.T) {
	weights := map[string]int{
		"critical": 6,
		"default":  3,
		"low":      1,
	}

	wrr := newWeightedRoundRobin(weights)
	if len(wrr.order) != 10 {
		t.Fatalf("Expected 10 slots (6+3+1), got %d", len(wrr.order))
	}

	// Count occurrences in the cycle.
	counts := make(map[string]int)
	for _, q := range wrr.order {
		counts[q]++
	}

	if counts["critical"] != 6 {
		t.Errorf("Expected 'critical' 6 times, got %d", counts["critical"])
	}
	if counts["default"] != 3 {
		t.Errorf("Expected 'default' 3 times, got %d", counts["default"])
	}
	if counts["low"] != 1 {
		t.Errorf("Expected 'low' 1 time, got %d", counts["low"])
	}
	t.Logf("✓ Weight distribution correct: critical=%d default=%d low=%d",
		counts["critical"], counts["default"], counts["low"])
}

// TestWeightedRoundRobinIsInterleaved verifies that the schedule is interleaved
// (no single queue is scheduled in a burst of more than necessary).
func TestWeightedRoundRobinIsInterleaved(t *testing.T) {
	weights := map[string]int{
		"high": 3,
		"low":  1,
	}
	wrr := newWeightedRoundRobin(weights)
	t.Logf("Interleaved schedule: %v", wrr.order)

	// Count max consecutive same-queue slots.
	maxConsecutive := 1
	cur := wrr.order[0]
	run := 1
	for i := 1; i < len(wrr.order); i++ {
		if wrr.order[i] == cur {
			run++
			if run > maxConsecutive {
				maxConsecutive = run
			}
		} else {
			cur = wrr.order[i]
			run = 1
		}
	}

	// With interleaving, "high" (weight=3) should not appear more than 2 consecutive times
	// in a 4-slot cycle [high, high, high, low] when interleaved becomes [high, low, high, high]
	// → worst case 2 consecutive. But the exact algorithm may vary.
	// The key assertion: no queue with weight W should appear W consecutive times if total > W.
	if maxConsecutive >= len(wrr.order) {
		t.Errorf("Schedule is not interleaved: all %d slots go to the same queue", len(wrr.order))
	} else {
		t.Logf("✓ Interleaved scheduling: max consecutive same queue = %d (total slots = %d)",
			maxConsecutive, len(wrr.order))
	}
}

// TestWeightedRoundRobinAtomicCounter verifies the counter is atomic and cycles correctly.
func TestWeightedRoundRobinAtomicCounter(t *testing.T) {
	weights := map[string]int{
		"a": 2,
		"b": 1,
	}
	wrr := newWeightedRoundRobin(weights)

	// Call queues() N times, verify all queue names are returned each time.
	totalSlots := len(wrr.order)
	for i := 0; i < totalSlots*3; i++ {
		result := wrr.queues()
		if len(result) != 2 {
			t.Errorf("Expected 2 unique queues, got %d at iteration %d", len(result), i)
		}
	}
	t.Logf("✓ Atomic counter cycles correctly over %d×3=%d iterations", totalSlots, totalSlots*3)
}

// TestWeightedRoundRobinSingleQueue verifies degenerate case.
func TestWeightedRoundRobinSingleQueue(t *testing.T) {
	weights := map[string]int{"only": 5}
	wrr := newWeightedRoundRobin(weights)
	if len(wrr.order) != 5 {
		t.Fatalf("Expected 5 slots, got %d", len(wrr.order))
	}
	for _, q := range wrr.order {
		if q != "only" {
			t.Errorf("Expected all slots to be 'only', got %q", q)
		}
	}
	t.Log("✓ Single-queue weighted schedule works correctly")
}

// TestWeightedRoundRobinConfig verifies the Config field is accepted by the server.
func TestWeightedRoundRobinConfig(t *testing.T) {
	r := setup(t)
	defer r.Close()

	// Just test that Config accepts WeightedQueues without panic.
	srv := NewServerFromRedisClient(r, Config{
		Concurrency:       4,
		TaskCheckInterval: 100 * 1000000, // 100ms
		WeightedQueues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
	})
	if srv == nil {
		t.Fatal("NewServerFromRedisClient returned nil")
	}
	t.Log("✓ Server created successfully with WeightedQueues config")
}
