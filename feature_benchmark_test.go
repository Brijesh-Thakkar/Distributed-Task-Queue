// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package asynq — Comprehensive Benchmark Suite.
//
// Run with: go test -bench=. -benchtime=10s -run='^$' .
//
// Benchmarks cover:
//   - BenchmarkEnqueue: raw enqueue throughput
//   - BenchmarkEnqueueParallel: concurrent enqueue throughput
//   - BenchmarkProcessing: end-to-end enqueue + process throughput
//   - BenchmarkWeightedPriorityScheduling: scheduling overhead with 3 queues
//   - BenchmarkIdempotencyCheck: overhead of idempotency Redis round-trip
//   - BenchmarkDLQSendTask: overhead of routing a task to DLQ
//   - BenchmarkVisibilityTracker: overhead of claim/release per task

package asynq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/redis/go-redis/v9"
)

// benchRedis creates a Redis client for benchmarks, skipping if Redis is unavailable.
func benchRedis(b *testing.B) redis.UniversalClient {
	b.Helper()
	r := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   redisDB,
	})
	if err := r.Ping(context.Background()).Err(); err != nil {
		b.Skip("Redis not available:", err)
	}
	// Flush test DB.
	r.FlushDB(context.Background())
	return r
}

// BenchmarkEnqueue measures raw task enqueue throughput.
func BenchmarkEnqueue(b *testing.B) {
	r := benchRedis(b)
	defer r.Close()

	client := NewClientFromRedisClient(r)
	defer client.Close()

	task := NewTask("benchmark:enqueue", []byte(`{"n":1}`))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Enqueue(task, TaskID(fmt.Sprintf("bench-enq-%d", i))); err != nil {
			b.Fatalf("Enqueue error: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
}

// BenchmarkEnqueueParallel measures concurrent enqueue throughput.
func BenchmarkEnqueueParallel(b *testing.B) {
	r := benchRedis(b)
	defer r.Close()

	client := NewClientFromRedisClient(r)
	defer client.Close()

	var counter atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			task := NewTask("benchmark:parallel", []byte(`{"n":1}`),
				TaskID(fmt.Sprintf("bench-par-%d", n)))
			if _, err := client.Enqueue(task); err != nil && err != ErrTaskIDConflict {
				b.Errorf("Enqueue error: %v", err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
}

// BenchmarkProcessing measures end-to-end task processing throughput (enqueue + process).
func BenchmarkProcessing(b *testing.B) {
	r := benchRedis(b)
	defer r.Close()

	client := NewClientFromRedisClient(r)
	defer client.Close()

	var processed atomic.Int64
	done := make(chan struct{})

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		n := processed.Add(1)
		if n >= int64(b.N) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	})

	srv := NewServerFromRedisClient(r, Config{
		Concurrency:       50,
		Queues:            map[string]int{"default": 1},
		TaskCheckInterval: 10 * time.Millisecond,
	})
	if err := srv.Start(handler); err != nil {
		b.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Shutdown()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		task := NewTask("benchmark:processing", []byte(`{"n":1}`),
			TaskID(fmt.Sprintf("bench-proc-%d", i)))
		if _, err := client.Enqueue(task); err != nil && err != ErrTaskIDConflict {
			b.Fatalf("Enqueue error: %v", err)
		}
	}

	// Wait for all tasks to be processed.
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		b.Fatalf("Timeout: only %d/%d tasks processed", processed.Load(), b.N)
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")
}

// BenchmarkWeightedPriorityScheduling measures the overhead of weighted round-robin scheduling.
func BenchmarkWeightedPriorityScheduling(b *testing.B) {
	weights := map[string]int{
		"critical": 6,
		"default":  3,
		"low":      1,
	}
	wrr := newWeightedRoundRobin(weights)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = wrr.queues()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "calls/sec")
}

// BenchmarkIdempotencyCheck measures the overhead of the atomic Redis SET NX operation.
func BenchmarkIdempotencyCheck(b *testing.B) {
	r := benchRedis(b)
	defer r.Close()

	checker := NewIdempotencyChecker(r)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-idem-%d", i)
		_, err := checker.CheckAndSet(ctx, key, time.Hour)
		if err != nil {
			b.Fatalf("CheckAndSet error: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "checks/sec")
}

// BenchmarkDLQSendTask measures the overhead of routing a task to the DLQ.
func BenchmarkDLQSendTask(b *testing.B) {
	r := benchRedis(b)
	defer r.Close()

	mgr := newDLQManager(r)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &base.TaskMessage{
			ID:      fmt.Sprintf("bench-dlq-%d", i),
			Type:    "bench:task",
			Payload: []byte(`{}`),
			Queue:   "default",
			Retry:   3,
			Retried: 3,
		}
		if err := mgr.sendToDLQ(ctx, "default", msg); err != nil {
			b.Fatalf("sendToDLQ error: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "sends/sec")
}

// BenchmarkVisibilityTracker measures claim + release overhead.
func BenchmarkVisibilityTracker(b *testing.B) {
	r := benchRedis(b)
	defer r.Close()

	tracker := NewVisibilityTracker(r, 30*time.Second)
	ctx := context.Background()
	_ = ctx

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		taskID := fmt.Sprintf("bench-vis-%d", i)
		if err := tracker.Claim(taskID); err != nil {
			b.Fatalf("Claim error: %v", err)
		}
		tracker.Release(taskID)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}
