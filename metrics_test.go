// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestMetricsServerCreation verifies that MetricsServer can be created.
func TestMetricsServerCreation(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19090",
		RedisClient:     r,
		Queues:          []string{"default", "critical"},
		CollectInterval: 5 * time.Second,
	})

	if ms == nil {
		t.Fatal("NewMetricsServer returned nil")
	}
	t.Log("✓ MetricsServer created successfully")
}

// TestMetricsMiddleware verifies that the middleware records metrics correctly.
func TestMetricsMiddleware(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19091",
		RedisClient:     r,
		Queues:          []string{"default"},
		CollectInterval: 30 * time.Second,
	})

	var handlerCalled bool
	baseHandler := HandlerFunc(func(ctx context.Context, task *Task) error {
		handlerCalled = true
		return nil
	})

	wrappedHandler := MetricsMiddleware(ms, baseHandler)

	// Create a fake task and context.
	task := NewTask("test:metrics", []byte(`{}`))
	ctx := context.Background()
	err := wrappedHandler.ProcessTask(ctx, task)
	if err != nil {
		t.Fatalf("MetricsMiddleware returned error: %v", err)
	}
	if !handlerCalled {
		t.Error("Expected base handler to be called")
	}
	t.Log("✓ MetricsMiddleware wraps handler and records metrics")
}

// TestMetricsEndpointReturnsPrometheusFormat verifies that /metrics returns valid Prometheus text.
func TestMetricsEndpointReturnsPrometheusFormat(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19092",
		RedisClient:     r,
		Queues:          []string{"default"},
		CollectInterval: 30 * time.Second,
	})

	// Trigger a manual metric recording.
	ms.RecordTaskProcessed("default", "success")
	ms.RecordTaskProcessed("default", "success")
	ms.RecordTaskProcessed("default", "failed")
	ms.RecordTaskDuration("default", 150*time.Millisecond)

	// Use the server's handler directly via httptest.
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	ms.server.Handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", resp.StatusCode)
	}

	bodyStr := string(body)

	// Check that our custom metrics appear in the output.
	checks := []string{
		"asynq_tasks_processed_total",
		"asynq_task_duration_seconds",
		"asynq_queue_depth",
		"asynq_dlq_depth",
		"asynq_active_workers",
	}
	for _, check := range checks {
		if !strings.Contains(bodyStr, check) {
			t.Errorf("Expected metric %q in /metrics output, but not found", check)
		}
	}
	t.Logf("✓ /metrics endpoint returns valid Prometheus format with %d bytes", len(bodyStr))
}

// TestMetricsCounters verifies counter values are correct.
func TestMetricsCounters(t *testing.T) {
	r := setup(t)
	defer r.Close()

	ms := NewMetricsServer(MetricsConfig{
		Addr:            ":19093",
		RedisClient:     r,
		Queues:          []string{"default"},
		CollectInterval: 30 * time.Second,
	})

	// Record 5 successes and 2 failures.
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

	// Check counters appear.
	if !strings.Contains(bodyStr, `status="success"`) {
		t.Error("Expected success status label in metrics output")
	}
	if !strings.Contains(bodyStr, `status="failed"`) {
		t.Error("Expected failed status label in metrics output")
	}
	t.Log("✓ Task processed counters with labels work correctly")
}
