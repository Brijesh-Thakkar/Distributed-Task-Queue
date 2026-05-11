# Distributed Task Queue

## Overview

This project is a **production-enhanced fork of [hibiken/asynq](https://github.com/hibiken/asynq)** — a reliable, Redis-backed distributed task queue for Go. While the upstream library provides solid primitives for background job processing, this fork adds five production-critical capabilities missing from the original: **exactly-once processing** via atomic idempotency keys, a **Dead Letter Queue (DLQ)** with HTTP inspection for permanently failed tasks, **visibility timeouts** with heartbeat-based crash recovery, **weighted priority queues** using deterministic round-robin scheduling, and a **Prometheus metrics endpoint** for real-time queue health monitoring. All features are implemented in Go, persist state in Redis, and integrate transparently with the existing `asynq.Server` and `asynq.Client` APIs.

---

## Architecture

```
┌─────────────┐     Enqueue()      ┌───────────────────┐
│   Client    │ ─────────────────► │   Redis Broker    │
│  (producer) │                    │  (sorted sets,    │
└─────────────┘                    │   lists, hashes)  │
                                   └────────┬──────────┘
                                            │ dequeue (BLPOP)
                                   ┌────────▼──────────┐
                                   │   Worker Pool     │
                                   │  (N goroutines)   │
                                   │                   │
                                   │  ┌─────────────┐  │
                                   │  │ Idempotency │  │
                                   │  │   Check     │  │
                                   │  └──────┬──────┘  │
                                   │         │          │
                                   │  ┌──────▼──────┐  │
                                   │  │  Visibility │  │
                                   │  │   Timeout   │  │
                                   │  │ (heartbeat) │  │
                                   │  └──────┬──────┘  │
                                   └─────────┼──────────┘
                                             │
                              ┌──────────────┴─────────────┐
                              │                            │
                      ┌───────▼──────┐            ┌───────▼──────┐
                      │   Handler    │            │  On Failure  │
                      │  (success)   │            │  (retries    │
                      └──────────────┘            │   exhausted) │
                                                  └───────┬──────┘
                                                          │ DLQThreshold exceeded
                                                  ┌───────▼──────┐
                                                  │     DLQ      │
                                                  │ Redis sorted │
                                                  │     set      │
                                                  └───────┬──────┘
                                                          │
                                                  ┌───────▼──────┐
                                                  │ HTTP Server  │
                                                  │ GET  /dlq/{q}│
                                                  │ POST requeue │
                                                  └──────────────┘

Prometheus metrics exposed at :9090/metrics
```

---

## Features

### 1. Exactly-Once Processing (Idempotency)

**What it solves:** In distributed systems, network retries and at-least-once delivery guarantees can cause the same task to be processed multiple times — charging a customer twice, sending duplicate emails, or double-counting inventory.

**How it works:** When a task is enqueued with an `IdempotencyKey`, a Redis Lua script atomically checks whether that key was already processed (using `SET NX`). If the key exists, the worker skips the task without calling the handler. The Lua script guarantees atomicity — no two workers can race to process the same task, even under high concurrency across multiple machines.

```go
client := asynq.NewClient(redisOpt)

task := asynq.NewTask("payment:charge", payload)
_, err := client.Enqueue(task,
    asynq.IdempotencyKey("charge-order-42-v1"),
    asynq.IdempotencyTTL(24*time.Hour),
)
```

---

### 2. Dead Letter Queue (DLQ)

**What it solves:** When a task permanently fails (e.g., malformed payload, downstream service outage), the upstream `asynq` silently archives it with no visibility or recovery path. Teams have no way to inspect what failed or retry tasks after fixing the root cause.

**How it works:** After a configurable number of retries (`DLQThreshold`), tasks are routed to a separate Redis sorted set (`asynq:{queue}:dlq`) instead of the archive. An HTTP handler provides inspection (`GET /dlq/{queue}`) and recovery (`POST /dlq/{queue}/{task_id}/requeue`) endpoints, enabling operators to review failures and reprocess them without code changes.

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    DLQThreshold: 3, // route to DLQ after 3 retries
})

// Mount the DLQ HTTP handler on any mux:
http.Handle("/dlq/", asynq.DLQHTTPHandler(redisClient))

// Inspect:  GET  /dlq/default
// Requeue:  POST /dlq/default/{task_id}/requeue
```

---

### 3. Visibility Timeouts + Crash Recovery

**What it solves:** If a worker process crashes mid-task (OOM kill, machine failure, SIGKILL), the task is stuck in "active" state forever — it will never be retried or completed. The upstream `asynq` relies on lease timeouts but has no heartbeat mechanism for long-running tasks.

**How it works:** When a worker picks up a task, `VisibilityTracker` sets a Redis key with a TTL equal to `VisibilityTimeout`. A background goroutine renews the key every 10 seconds (heartbeat). If the process crashes, the key expires and a background `recoverer` detects orphaned tasks and moves them back to the pending queue for reprocessing — completely automatically.

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    VisibilityTimeout: 30 * time.Second, // task must renew every 10s
})
// No other code changes needed — crash recovery is automatic.
```

---

### 4. Weighted Priority Queues

**What it solves:** The upstream `asynq` multi-queue support uses random queue selection weighted by priority, which produces non-deterministic distribution — over short windows, critical queues may be starved. There is no guarantee that a queue with weight 6 gets exactly 6x more slots than weight 1.

**How it works:** `WeightedQueues` pre-computes a deterministic polling order using the [Smooth Weighted Round-Robin](https://github.com/nginx/nginx/commit/52327e0627f49dbda1e8db695e63a4b0af4448b3) algorithm. The weight map is expanded into a cycle slice (e.g., `[critical, critical, default, critical, default, low, ...]`) and traversed sequentially using an atomic counter — guaranteeing the exact configured distribution across any window of N total polls.

```go
srv := asynq.NewServer(redisOpt, asynq.Config{
    WeightedQueues: map[string]int{
        "critical": 6, // 60% of polling slots
        "default":  3, // 30% of polling slots
        "low":      1, // 10% of polling slots
    },
})
```

---

### 5. Prometheus Metrics

**What it solves:** Production systems need real-time visibility into queue depth, processing throughput, and latency percentiles. Without metrics, teams can't detect backlogs, SLA violations, or the impact of code changes on performance.

**How it works:** `MetricsMiddleware` wraps every handler invocation and records duration and success/failure in Prometheus counters and histograms. A background goroutine polls Redis every `CollectInterval` seconds to update queue depth gauges. The `MetricsServer` exposes a `/metrics` endpoint on a configurable port in the standard Prometheus text format, ready for Grafana dashboards or alerting.

**Metrics exposed:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `asynq_tasks_processed_total` | Counter | `queue`, `status` | Cumulative tasks processed |
| `asynq_task_duration_seconds` | Histogram | `queue` | Processing latency (p50/p95/p99) |
| `asynq_queue_depth` | Gauge | `queue` | Pending tasks count |
| `asynq_active_tasks` | Gauge | `queue` | Currently processing |
| `asynq_dlq_depth` | Gauge | `queue` | Tasks in Dead Letter Queue |
| `asynq_active_workers` | Gauge | — | Active worker goroutines |

```go
ms := asynq.NewMetricsServer(asynq.MetricsConfig{
    Addr:            ":9090",
    RedisClient:     redisClient,
    Queues:          []string{"critical", "default", "low"},
    CollectInterval: 5 * time.Second,
})
go ms.Start() // serves :9090/metrics and :9090/healthz

// Wrap your handler:
srv.Start(asynq.MetricsMiddleware(ms, myHandler))
```

---

## Performance

All benchmarks run on **13th Gen Intel Core i5-13420H**, Redis 8.6.3 (local, single-node), Go 1.24.0, Linux.

### Benchmark Results

```
goos: linux
goarch: amd64
pkg: github.com/hibiken/asynq
cpu: 13th Gen Intel(R) Core(TM) i5-13420H
BenchmarkEnqueue-12                       	  201538	     26522 ns/op	     37705 tasks/sec	    1464 B/op	      27 allocs/op
BenchmarkEnqueueParallel-12               	  543436	     13004 ns/op	     76897 tasks/sec	    1475 B/op	      28 allocs/op
BenchmarkWeightedPriorityScheduling-12    	27285434	       225.7 ns/op	   4430417 calls/sec	     112 B/op	       3 allocs/op
BenchmarkIdempotencyCheck-12              	  236122	     22869 ns/op	     43728 checks/sec	     412 B/op	      15 allocs/op
BenchmarkDLQSendTask-12                   	  238436	     21476 ns/op	     46564 sends/sec	     704 B/op	      16 allocs/op
BenchmarkVisibilityTracker-12             	   97326	     60357 ns/op	     16568 ops/sec	     968 B/op	      25 allocs/op
PASS
ok  	github.com/hibiken/asynq	37.032s
```

**Interpretation:**
- `BenchmarkEnqueue` — **37,705 tasks/sec** single-goroutine; each call is one Redis round-trip (ZADD + HSET).
- `BenchmarkEnqueueParallel` — **76,897 tasks/sec** across 12 goroutines; near-linear scaling with parallelism.
- `BenchmarkWeightedPriorityScheduling` — **4.4M calls/sec** at 225ns/op; the round-robin scheduler adds negligible overhead vs. random selection.
- `BenchmarkIdempotencyCheck` — **43,728 checks/sec**; atomic Lua dedup adds ~23µs per task — one Redis round-trip with scripted NX check.
- `BenchmarkDLQSendTask` — **46,564 sends/sec**; DLQ routing overhead is one additional Redis ZADD.
- `BenchmarkVisibilityTracker` — **16,568 ops/sec**; claim + release costs two Redis SET/DEL operations (~60µs).

---

### Load Test Results (10,000 tasks, 50 workers)

```
════════════════════════════════════════════════════════════
  Distributed Task Queue — Load Test
════════════════════════════════════════════════════════════
  Redis:    localhost:6379
  Tasks:    10000
  Workers:  50
────────────────────────────────────────────────────────────

  Enqueueing 10000 tasks... done in 895ms
  Processing tasks with 50 workers...

════════════════════════════════════════════════════════════
  RESULTS
════════════════════════════════════════════════════════════
  Total tasks enqueued:          10000
  Tasks processed:               10000
  Tasks failed:                  0
────────────────────────────────────────────────────────────
  Enqueue duration:              895ms
  Enqueue throughput:            11176 tasks/sec
────────────────────────────────────────────────────────────
  Processing duration:           40ms
  Processing throughput:         250782 tasks/sec
────────────────────────────────────────────────────────────
  Latency p50:                   35ms
  Latency p95:                   48ms
  Latency p99:                   50ms
════════════════════════════════════════════════════════════

  Machine: 13th Gen Intel(R) Core(TM) i5-13420H
```

**Interpretation:** 10,000 tasks enqueue in under 1 second (~11k tasks/sec). With 50 concurrent workers and a no-op handler, all tasks are processed in **40ms** (~250k tasks/sec throughput). End-to-end latency (enqueue → handler return) is **35ms p50 / 50ms p99** — dominated by Redis polling interval (10ms default) and scheduling overhead.

---

## Quick Start

**Prerequisites:** Go 1.21+, Redis 6+

```bash
git clone https://github.com/YOUR_USERNAME/Distributed-Task-Queue
cd Distributed-Task-Queue
go mod tidy
```

**Minimal working example:**

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
)

func main() {
    redisOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}

    // --- Producer ---
    c := asynq.NewClient(redisOpt)
    defer c.Close()

    task := asynq.NewTask("email:send", []byte(`{"to":"user@example.com"}`))
    if _, err := c.Enqueue(task,
        asynq.IdempotencyKey("email-welcome-user-1"),
        asynq.IdempotencyTTL(24*time.Hour),
    ); err != nil {
        log.Fatal(err)
    }

    // --- Consumer ---
    srv := asynq.NewServer(redisOpt, asynq.Config{
        Concurrency:       10,
        DLQThreshold:      3,
        VisibilityTimeout: 30 * time.Second,
        WeightedQueues:    map[string]int{"default": 1},
    })

    mux := asynq.NewServeMux()
    mux.HandleFunc("email:send", func(ctx context.Context, t *asynq.Task) error {
        fmt.Printf("Sending email: %s\n", t.Payload())
        return nil
    })

    log.Fatal(srv.Run(mux))
}
```

---

## Running Tests

```bash
# Feature tests only — fast (~20 seconds), requires Redis on :6379
go test -run '^(TestWeighted|TestIdempotency|TestDLQ|TestVisibility|TestMetrics)' \
    -timeout=60s -v .

# Benchmarks — requires Redis on :6379
go test -bench=. -benchtime=5s -run='^$' .

# Load test — 10k tasks, 50 concurrent workers
go run cmd/loadtest/main.go -tasks=10000 -workers=50

# Load test with custom parameters
go run cmd/loadtest/main.go -tasks=100000 -workers=100 -redis=localhost:6379
```

---

## Project Structure

Files added by this fork (all upstream files are unchanged):

| File | Description |
|------|-------------|
| `idempotency.go` | Redis Lua idempotency checker — atomic `SET NX` dedup |
| `dlq.go` | DLQ manager + HTTP inspection endpoints (`/dlq/{queue}`) |
| `visibility.go` | Visibility timeout tracker with 10s heartbeat renewal |
| `weighted_queue.go` | Deterministic weighted round-robin queue scheduler |
| `metrics.go` | Prometheus `MetricsServer` + `MetricsMiddleware` wrapper |
| `cmd/loadtest/main.go` | Load test CLI: configurable tasks/workers, p50/p95/p99 output |
| `idempotency_test.go` | Tests for exactly-once semantics (dedup, TTL, race) |
| `dlq_test.go` | Tests for DLQ routing after threshold and direct list/send |
| `recoverer_visibility_test.go` | Tests for TTL expiry, heartbeat renewal, crash recovery |
| `weighted_queue_test.go` | Tests for round-robin distribution, interleaving, atomicity |
| `metrics_test.go` | Tests for Prometheus endpoint format and counter labels |
| `feature_benchmark_test.go` | Benchmarks for all 5 features with `tasks/sec` reporting |

---

## Configuration Reference

```go
asynq.Config{
    // Standard asynq fields
    Concurrency: 10,
    Queues:      map[string]int{"default": 1},

    // Feature 1: Idempotency (set per-task at Enqueue time)
    // asynq.IdempotencyKey("key"), asynq.IdempotencyTTL(24*time.Hour)

    // Feature 2: Dead Letter Queue
    DLQThreshold: 3, // route to DLQ after 3 retries (default: 3)

    // Feature 3: Visibility Timeout
    VisibilityTimeout: 30 * time.Second, // heartbeat every 10s

    // Feature 4: Weighted Priority Queues
    WeightedQueues: map[string]int{
        "critical": 6,
        "default":  3,
        "low":      1,
    },

    // Feature 5: Metrics (configured separately via MetricsServer)
}
```
