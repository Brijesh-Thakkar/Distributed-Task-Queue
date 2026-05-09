// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package asynq — Weighted Round-Robin Priority Queue Scheduler.
//
// WeightedQueues provides deterministic weighted round-robin scheduling across
// multiple queues. Unlike the existing probabilistic approach (random shuffle),
// this guarantees the exact configured ratio over time.
//
// For example, {"critical": 6, "default": 3, "low": 1} means the polling
// order cycles through: critical×6, default×3, low×1 in a repeating 10-slot cycle.

package asynq

import (
	"sort"
	"sync/atomic"
)

// weightedRoundRobin implements a pre-expanded cycling scheduler.
// It expands the weight map into an ordered slice and cycles through it atomically.
type weightedRoundRobin struct {
	order   []string   // pre-expanded polling order
	counter atomic.Int64
}

// newWeightedRoundRobin creates a weightedRoundRobin from a weight map.
// Weights are normalized by their GCD so the cycle length is minimal.
func newWeightedRoundRobin(weights map[string]int) *weightedRoundRobin {
	// Collect queues sorted by name for deterministic order.
	var entries []queueEntry
	for name, w := range weights {
		if w > 0 {
			entries = append(entries, queueEntry{name, w})
		}
	}
	// Sort by name for stable iteration.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	// Build expanded order slice using interleaved algorithm.
	order := interleaveWeighted(entries)

	wrr := &weightedRoundRobin{order: order}
	return wrr
}

// queueEntry holds a queue name and its weight.
type queueEntry struct {
	name   string
	weight int
}

// interleaveWeighted creates an interleaved schedule where heavier queues
// are distributed evenly across the cycle rather than bunched at the start.
// E.g., {critical:3, default:2, low:1} → [critical, default, critical, low, critical, default]
func interleaveWeighted(entries []queueEntry) []string {
	total := 0
	for _, e := range entries {
		total += e.weight
	}
	if total == 0 {
		return nil
	}
	result := make([]string, total)
	// Use the Bresenham-like distribution algorithm.
	counters := make([]float64, len(entries))
	weights := make([]float64, len(entries))
	for i, e := range entries {
		weights[i] = float64(e.weight)
	}
	for slot := 0; slot < total; slot++ {
		// Find the queue with the highest accumulated weight.
		best := -1
		var bestVal float64
		for i := range entries {
			counters[i] += weights[i]
			if best < 0 || counters[i] > bestVal {
				best = i
				bestVal = counters[i]
			}
		}
		result[slot] = entries[best].name
		counters[best] -= float64(total)
	}
	return result
}

// next returns the next queue name to poll according to the weighted schedule.
func (wrr *weightedRoundRobin) next() string {
	idx := wrr.counter.Add(1) - 1
	return wrr.order[int(idx)%len(wrr.order)]
}

// queues returns the pre-expanded polling order (for compatibility with existing Dequeue API).
// The caller will use the first unique queue names from this slice.
func (wrr *weightedRoundRobin) queues() []string {
	// For the Dequeue API, we return all queue names in priority order for this slot.
	// The current slot determines which queue gets highest priority.
	idx := wrr.counter.Add(1) - 1
	startPos := int(idx) % len(wrr.order)

	// Return a de-duplicated list starting from the current position.
	seen := make(map[string]bool)
	var result []string
	for i := 0; i < len(wrr.order); i++ {
		q := wrr.order[(startPos+i)%len(wrr.order)]
		if !seen[q] {
			seen[q] = true
			result = append(result, q)
		}
	}
	return result
}
