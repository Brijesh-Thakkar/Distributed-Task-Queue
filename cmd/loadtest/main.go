// Copyright 2024 dtq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// cmd/loadtest/main.go — Distributed Task Queue Load Test
//
// Fires 10,000 tasks with configurable concurrent workers.
// Measures total time, tasks/second throughput, and p50/p95/p99 latency.
//
// Usage:
//
//	go run cmd/loadtest/main.go
//	go run cmd/loadtest/main.go -tasks=50000 -workers=100 -redis=localhost:6379
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brijesh-thakkar/distributed-task-queue"
	"github.com/redis/go-redis/v9"
)

func main() {
	// CLI flags
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	numTasks := flag.Int("tasks", 10000, "Number of tasks to enqueue")
	numWorkers := flag.Int("workers", 50, "Number of concurrent workers")
	flag.Parse()

	fmt.Println(strings.Repeat("═", 60))
	fmt.Println("  Distributed Task Queue — Load Test")
	fmt.Println(strings.Repeat("═", 60))
	fmt.Printf("  Redis:    %s\n", *redisAddr)
	fmt.Printf("  Tasks:    %d\n", *numTasks)
	fmt.Printf("  Workers:  %d\n", *numWorkers)
	fmt.Println(strings.Repeat("─", 60))

	// Connect to Redis
	r := redis.NewClient(&redis.Options{Addr: *redisAddr, DB: 15})
	if err := r.Ping(context.Background()).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Cannot connect to Redis at %s: %v\n", *redisAddr, err)
		os.Exit(1)
	}
	// Use a fresh DB for the load test.
	r.FlushDB(context.Background())

	// Per-task latency tracking (nanoseconds)
	var mu sync.Mutex
	latencies := make([]int64, 0, *numTasks)
	var processed atomic.Int64
	var failCount atomic.Int64

	// Shared start time sent via payload
	taskStart := make(map[string]time.Time, *numTasks)
	var taskStartMu sync.Mutex

	done := make(chan struct{})

	handler := dtq.HandlerFunc(func(ctx context.Context, task *dtq.Task) error {
		taskStartMu.Lock()
		start, ok := taskStart[task.Type()]
		taskStartMu.Unlock()
		if ok {
			latNs := time.Since(start).Nanoseconds()
			mu.Lock()
			latencies = append(latencies, latNs)
			mu.Unlock()
		}
		n := processed.Add(1)
		if n >= int64(*numTasks) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	})

	srv := dtq.NewServer(
		dtq.RedisClientOpt{Addr: *redisAddr, DB: 15},
		dtq.Config{
			Concurrency:       *numWorkers,
			Queues:            map[string]int{"loadtest": 1},
			TaskCheckInterval: 10 * time.Millisecond,
			LogLevel:          dtq.FatalLevel, // suppress server logs during load test
		},
	)
	if err := srv.Start(handler); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to start server: %v\n", err)
		os.Exit(1)
	}
	defer srv.Shutdown()

	client := dtq.NewClient(dtq.RedisClientOpt{Addr: *redisAddr, DB: 15})
	defer client.Close()

	fmt.Printf("\n  Enqueueing %d tasks... ", *numTasks)
	enqStart := time.Now()

	// Enqueue all tasks, recording per-task start time keyed by type
	for i := 0; i < *numTasks; i++ {
		taskType := fmt.Sprintf("loadtest:task:%d", i)
		now := time.Now()
		taskStartMu.Lock()
		taskStart[taskType] = now
		taskStartMu.Unlock()

		task := dtq.NewTask(taskType, []byte(`{}`),
			dtq.Queue("loadtest"),
			dtq.MaxRetry(0),
		)
		if _, err := client.Enqueue(task); err != nil {
			failCount.Add(1)
		}
	}
	enqDuration := time.Since(enqStart)
	fmt.Printf("done in %v\n", enqDuration.Round(time.Millisecond))

	// Wait for all tasks to complete
	fmt.Printf("  Processing tasks with %d workers...\n", *numWorkers)
	procStart := time.Now()

	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		fmt.Fprintf(os.Stderr, "\nERROR: Timeout — only %d/%d tasks processed\n",
			processed.Load(), *numTasks)
		os.Exit(1)
	}
	totalDuration := time.Since(procStart)

	// Calculate statistics
	mu.Lock()
	lats := make([]int64, len(latencies))
	copy(lats, latencies)
	mu.Unlock()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	var p50, p95, p99 time.Duration
	if len(lats) > 0 {
		p50 = time.Duration(lats[int(float64(len(lats))*0.50)])
		p95 = time.Duration(lats[int(float64(len(lats))*0.95)])
		p99Idx := int(float64(len(lats)) * 0.99)
		if p99Idx >= len(lats) {
			p99Idx = len(lats) - 1
		}
		p99 = time.Duration(lats[p99Idx])
	}

	throughput := float64(processed.Load()) / totalDuration.Seconds()
	enqThroughput := float64(*numTasks) / enqDuration.Seconds()

	// Print results table
	fmt.Println()
	fmt.Println(strings.Repeat("═", 60))
	fmt.Println("  RESULTS")
	fmt.Println(strings.Repeat("═", 60))
	fmt.Printf("  %-30s %v\n", "Total tasks enqueued:", *numTasks)
	fmt.Printf("  %-30s %d\n", "Tasks processed:", processed.Load())
	fmt.Printf("  %-30s %d\n", "Tasks failed:", failCount.Load())
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  %-30s %v\n", "Enqueue duration:", enqDuration.Round(time.Millisecond))
	fmt.Printf("  %-30s %.0f tasks/sec\n", "Enqueue throughput:", enqThroughput)
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  %-30s %v\n", "Processing duration:", totalDuration.Round(time.Millisecond))
	fmt.Printf("  %-30s %.0f tasks/sec\n", "Processing throughput:", throughput)
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  %-30s %v\n", "Latency p50:", p50.Round(time.Millisecond))
	fmt.Printf("  %-30s %v\n", "Latency p95:", p95.Round(time.Millisecond))
	fmt.Printf("  %-30s %v\n", "Latency p99:", p99.Round(time.Millisecond))
	fmt.Println(strings.Repeat("═", 60))
	fmt.Printf("\n  Machine: %s\n", getMachineInfo())
}

func getMachineInfo() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "model name") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return "unknown"
}
