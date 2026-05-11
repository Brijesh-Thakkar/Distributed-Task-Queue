// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestMetricsServerCreation verifies MetricsServer initializes without error.
func TestMetricsServerCreation(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19090",
		RedisClient:     r,
		Queues:          []string{"default", "critical"},
		CollectInterval: 30 * time.Second,
	})
	if ms == nil {
		t.Fatal("NewMetricsServer returned nil")
	}
	t.Log("✓ MetricsServer created successfully")
}

// TestMetricsMiddleware verifies the middleware calls the underlying handler
// and does not return an error for a successful task.
func TestMetricsMiddleware(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19091",
		RedisClient:     r,
		Queues:          []string{"default"},
		CollectInterval: 30 * time.Second,
	})

	var called bool
	base := HandlerFunc(func(ctx context.Context, task *Task) error {
		called = true
		return nil
	})

	wrapped := MetricsMiddleware(ms, base)
	task := NewTask("metrics:test", []byte(`{}`))
	if err := wrapped.ProcessTask(context.Background(), task); err != nil {
		t.Fatalf("MetricsMiddleware returned error: %v", err)
	}
	if !called {
		t.Error("underlying handler was not called")
	}
	t.Log("✓ MetricsMiddleware wraps handler correctly")
}

// TestMetricsEndpointReturnsPrometheusFormat verifies that /metrics returns
// valid Prometheus text with all expected metric families.
//
// Prometheus only emits a metric family once it has been observed (counter
// incremented, gauge set, histogram observed). This test records observations
// for every family BEFORE making the HTTP request.
func TestMetricsEndpointReturnsPrometheusFormat(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19092",
		RedisClient:     r,
		Queues:          []string{"default"},
		CollectInterval: 30 * time.Second,
	})

	// --- Seed every metric family so they all appear in the output ---

	// counters + histogram (via public API)
	ms.RecordTaskProcessed("default", "success")
	ms.RecordTaskProcessed("default", "failed")
	ms.RecordTaskDuration("default", 50*time.Millisecond)
	ms.RecordTaskDuration("default", 200*time.Millisecond)

	// gauges (direct field access — all in same package)
	ms.metrics.queueDepth.WithLabelValues("default").Set(5)
	ms.metrics.activeTasks.WithLabelValues("default").Set(2)
	ms.metrics.retryTasks.WithLabelValues("default").Set(1)
	ms.metrics.archivedTasks.WithLabelValues("default").Set(0)
	ms.metrics.dlqDepth.WithLabelValues("default").Set(0)
	ms.metrics.activeWorkers.Set(2)

	// --- Make the HTTP request ---
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	ms.server.Handler.ServeHTTP(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	bodyStr := string(body)

	// Every family we registered must appear.
	want := []string{
		"asynq_tasks_processed_total",
		"asynq_task_duration_seconds",
		"asynq_active_workers",
		"asynq_queue_depth",
		"asynq_dlq_depth",
	}
	for _, name := range want {
		if !strings.Contains(bodyStr, name) {
			t.Errorf("metric %q missing from /metrics output", name)
		}
	}
	t.Logf("✓ /metrics returned %d bytes with all %d metric families", len(bodyStr), len(want))
}

// TestMetricsCounters verifies that counter labels (status) appear correctly.
func TestMetricsCounters(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19093",
		RedisClient:     r,
		Queues:          []string{"default"},
		CollectInterval: 30 * time.Second,
	})

	for i := 0; i < 5; i++ {
		ms.RecordTaskProcessed("default", "success")
	}
	for i := 0; i < 2; i++ {
		ms.RecordTaskProcessed("default", "failed")
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	ms.server.Handler.ServeHTTP(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, `status="success"`) {
		t.Error(`expected status="success" label in /metrics`)
	}
	if !strings.Contains(bodyStr, `status="failed"`) {
		t.Error(`expected status="failed" label in /metrics`)
	}
	t.Log("✓ task processed counters appear with correct labels")
}
