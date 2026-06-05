# Distributed Task Queue (DTQ)

## Overview

**Distributed Task Queue (DTQ)** is a high-performance, Redis-backed asynchronous 
task processing system built in Go. Designed for production-grade reliability and 
extreme observability, DTQ provides a robust framework for handling background jobs, 
scheduled tasks, and complex distributed workflows with minimal overhead.

Built to solve the most common pitfalls in distributed processing, DTQ is anchored 
by five core design pillars:

1. **Exactly-Once Processing** — Atomic idempotency keys using Redis Lua scripts to prevent duplicate executions.
2. **Dead Letter Queue (DLQ)** — Integrated failure isolation with HTTP-based inspection and requeueing.
3. **Visibility Timeouts** — Heartbeat-driven crash recovery that ensures zero task loss when workers die mid-execution.
4. **Weighted Priority Queues** — Deterministic Smooth Weighted Round-Robin (SWRR) scheduling for fair, predictable resource allocation.
5. **Native Observability** — Built-in Prometheus metrics for real-time monitoring of queue depth, latency, and worker health.

---

## Architecture

```
┌─────────────────────────────┐         Enqueue()          ┌─────────────────────────────┐
│           Client            │ ─────────────────────────► │         Redis Broker        │
│         (producer)          │                            │       (sorted sets,          │
│  .../distributed-task-queue │                            │       lists, hashes)         │
│          /client            │                            │  .../distributed-task-queue  │
└─────────────────────────────┘                            │          /broker             │
                                                           └──────────────┬──────────────┘
                                                                          │ dequeue (BLPOP)
                                                           ┌──────────────▼──────────────┐
                                                           │         Worker Pool         │
                                                           │       (N goroutines)        │
                                                           │  .../distributed-task-queue │
                                                           │          /worker            │
                                                           │                             │
                                                           │      ┌───────────────┐      │
                                                           │      │  Idempotency  │      │
                                                           │      │  (SET NX Lua) │      │
                                                           │      └───────┬───────┘      │
                                                           │              │              │
                                                           │      ┌───────▼───────┐      │
                                                           │      │  Visibility   │      │
                                                           │      │    Timeout    │      │
                                                           │      │  (heartbeat)  │      │
                                                           │      └───────┬───────┘      │
                                                           └──────────────┼──────────────┘
                                                                          │
                                              ┌───────────────────────────┴───────────────────────────┐
                                              │                                                       │
                                      ┌───────▼───────┐                                       ┌───────▼───────┐
                                      │    Handler    │                                       │  On Failure   │
                                      │   (success)   │                                       │   (retries    │
                                      └───────────────┘                                       │  exhausted)   │
                                                                                              └───────┬───────┘
                                                                                                      │ DLQThreshold exceeded
                                                                                              ┌───────▼───────┐
                                                                                              │      DLQ      │
                                                                                              │ Redis sorted  │
                                                                                              │      set      │
                                                                                              │   .../dlq     │
                                                                                              └───────┬───────┘
                                                                                                      │
                                                                                              ┌───────▼───────┐
                                                                                              │  HTTP Server  │
                                                                                              │ GET  /dlq/{q} │
                                                                                              │ POST requeue  │
                                                                                              └───────────────┘

Prometheus metrics exposed at :9090/metrics  (.../metrics)
```

---

## Design Decisions

- **Idempotency:** Redis Lua scripts execute `SET NX` atomically, ensuring a task is only accepted if its unique key does not already exist — eliminating race conditions across concurrent workers.
- **Dead Letter Queue:** Failed tasks are moved to a dedicated Redis Sorted Set rather than discarded. An HTTP management interface enables manual inspection of failed payloads and bulk requeue once root causes are resolved.
- **Visibility Timeouts:** A 10-second heartbeat lease detects worker crashes automatically. If a worker fails to check in, the task is marked orphaned and reclaimed by another worker after TTL expiry — with no manual intervention required.
- **Weighted Queues:** Deterministic Smooth Weighted Round-Robin guarantees exact configured distribution across any window of N polls — no starvation, no randomness.
- **Metrics:** A middleware-style collector hooks directly into the task lifecycle to export p99 latency histograms, queue depth gauges, and active worker counts to Prometheus with negligible hot-path overhead.

---

## Quick Start

**Prerequisites:** Go 1.21+, Redis 6+

```bash
git clone https://github.com/brijesh-thakkar/distributed-task-queue
cd distributed-task-queue
go mod tidy
```

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/brijesh-thakkar/distributed-task-queue/client"
    "github.com/brijesh-thakkar/distributed-task-queue/core"
    "github.com/brijesh-thakkar/distributed-task-queue/worker"
)

func main() {
    redisOpt := client.RedisClientOpt{Addr: "localhost:6379"}

    // --- Producer ---
    c := client.NewClient(redisOpt)
    defer c.Close()

    task := core.NewTask("email:send", []byte(`{"to":"user@example.com"}`))
    if _, err := c.Enqueue(task,
        core.IdempotencyKey("email-welcome-user-1"),
        core.IdempotencyTTL(24*time.Hour),
    ); err != nil {
        log.Fatal(err)
    }

    // --- Consumer ---
    srv := worker.NewServer(redisOpt, worker.Config{
        Concurrency:       10,
        DLQThreshold:      3,
        VisibilityTimeout: 30 * time.Second,
        WeightedQueues: map[string]int{
            "critical": 6,
            "default":  3,
            "low":      1,
        },
    })

    mux := worker.NewServeMux()
    mux.HandleFunc("email:send", func(ctx context.Context, t *core.Task) error {
        fmt.Printf("Sending email: %s\n", t.Payload())
        return nil
    })

    log.Fatal(srv.Run(mux))
}
```

---

## Performance

All benchmarks run on **13th Gen Intel Core i5-13420H**, Redis 8.6.3 (local, single-node), Go 1.24.0, Linux.

### Benchmark Results

```
BenchmarkEnqueue-12                       201538    26522 ns/op    37705 tasks/sec    1464 B/op    27 allocs/op
BenchmarkEnqueueParallel-12               543436    13004 ns/op    76897 tasks/sec    1475 B/op    28 allocs/op
BenchmarkWeightedPriorityScheduling-12  27285434      225.7 ns/op  4430417 calls/sec   112 B/op     3 allocs/op
BenchmarkIdempotencyCheck-12              236122    22869 ns/op    43728 checks/sec    412 B/op    15 allocs/op
BenchmarkDLQSendTask-12                   238436    21476 ns/op    46564 sends/sec     704 B/op    16 allocs/op
BenchmarkVisibilityTracker-12              97326    60357 ns/op    16568 ops/sec       968 B/op    25 allocs/op
```

### Load Test (10,000 tasks, 50 workers)

| Metric | Result |
|---|---|
| Enqueue duration | 895ms |
| Enqueue throughput | 11,176 tasks/sec |
| Processing duration | 40ms |
| Processing throughput | 250,782 tasks/sec |
| Latency p50 | 35ms |
| Latency p95 | 48ms |
| Latency p99 | 50ms |

---

## Running Tests

```bash
# Feature tests (requires Redis on :6379)
go test -run '^(TestWeighted|TestIdempotency|TestDLQ|TestVisibility|TestMetrics)' \
    -timeout=60s -v ./...

# Benchmarks
go test -bench=. -benchtime=5s -run='^$' ./...

# Load test
go run cmd/loadtest/main.go -tasks=10000 -workers=50
```

---

## Project Structure

| Package | Description |
|---|---|
| `client/` | Developer-facing API for enqueuing and scheduling tasks |
| `worker/` | Task execution engine, worker pool, and routing |
| `broker/` | Redis state sync, pub/sub, and background maintenance |
| `core/` | Foundational types: Task, Options, TaskInfo |
| `idempotency/` | Atomic exactly-once deduplication via Redis Lua |
| `dlq/` | Dead Letter Queue storage, HTTP inspection, and requeue |
| `visibility/` | Heartbeat-based visibility timeout and crash recovery |
| `queue/` | Smooth Weighted Round-Robin scheduler |
| `metrics/` | Prometheus exporter, histograms, and worker gauges |
| `internal/` | Private Redis client, base types, and test utilities |
| `cmd/loadtest/` | CLI load generator with p50/p95/p99 reporting |

---

## Configuration Reference

```go
worker.Config{
    Concurrency: 10,

    // Feature 1: set per-task at enqueue time
    // core.IdempotencyKey("key"), core.IdempotencyTTL(24*time.Hour)

    // Feature 2: Dead Letter Queue
    DLQThreshold: 3,

    // Feature 3: Visibility Timeout
    VisibilityTimeout: 30 * time.Second,

    // Feature 4: Weighted Priority Queues
    WeightedQueues: map[string]int{
        "critical": 6,
        "default":  3,
        "low":      1,
    },

    // Feature 5: Metrics (configured separately)
    // ms := metrics.NewServer(metrics.Config{Addr: ":9090", ...})
    // go ms.Start()
    // srv.Run(metrics.Middleware(ms, myHandler))
}
```