// Copyright 2024 dtq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package dtq — Prometheus Metrics Server.
//
// MetricsServer exposes real-time queue health metrics at the /metrics endpoint.
//
// Metrics exposed:
//   - dtq_queue_depth{queue}           Gauge     — pending tasks count
//   - dtq_active_tasks{queue}          Gauge     — currently processing tasks
//   - dtq_retry_tasks{queue}           Gauge     — tasks in retry state
//   - dtq_archived_tasks{queue}        Gauge     — tasks in archive
//   - dtq_dlq_depth{queue}             Gauge     — tasks in Dead Letter Queue
//   - dtq_tasks_processed_total{queue,status} Counter — cumulative tasks processed
//   - dtq_task_duration_seconds{queue} Histogram — processing latency
//   - dtq_active_workers               Gauge     — actively processing workers

package dtq

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	// MetricsNamespace is the Prometheus namespace for all dtq metrics.
	MetricsNamespace = "dtq"
)

// MetricsConfig configures the MetricsServer.
type MetricsConfig struct {
	// Addr is the listen address for the metrics HTTP server (e.g. ":9090").
	// Default: ":9090"
	Addr string

	// RedisClient is the Redis connection used to poll queue stats.
	RedisClient redis.UniversalClient

	// Queues is the list of queue names to track metrics for.
	Queues []string

	// CollectInterval specifies how often queue depth metrics are refreshed.
	// Default: 5 seconds.
	CollectInterval time.Duration
}

// MetricsServer exposes Prometheus metrics for dtq queue health.
// Start it in a goroutine alongside your dtq.Server:
//
//	ms := dtq.NewMetricsServer(dtq.MetricsConfig{...})
//	go ms.Start()
type MetricsServer struct {
	cfg     MetricsConfig
	reg     *prometheus.Registry
	server  *http.Server
	metrics *dtqMetrics
}

type dtqMetrics struct {
	queueDepth     *prometheus.GaugeVec
	activeTasks    *prometheus.GaugeVec
	retryTasks     *prometheus.GaugeVec
	archivedTasks  *prometheus.GaugeVec
	dlqDepth       *prometheus.GaugeVec
	processedTotal *prometheus.CounterVec
	taskDuration   *prometheus.HistogramVec
	activeWorkers  prometheus.Gauge
}

// NewMetricsServer creates a new MetricsServer.
func NewMetricsServer(cfg MetricsConfig) *MetricsServer {
	if cfg.Addr == "" {
		cfg.Addr = ":9090"
	}
	if cfg.CollectInterval <= 0 {
		cfg.CollectInterval = 5 * time.Second
	}

	reg := prometheus.NewRegistry()
	m := &dtqMetrics{
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "queue_depth",
			Help:      "Number of tasks currently pending in the queue.",
		}, []string{"queue"}),
		activeTasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "active_tasks",
			Help:      "Number of tasks currently being processed.",
		}, []string{"queue"}),
		retryTasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "retry_tasks",
			Help:      "Number of tasks scheduled for retry.",
		}, []string{"queue"}),
		archivedTasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "archived_tasks",
			Help:      "Number of tasks in the archive (failed, exhausted retries).",
		}, []string{"queue"}),
		dlqDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "dlq_depth",
			Help:      "Number of tasks in the Dead Letter Queue.",
		}, []string{"queue"}),
		processedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Name:      "tasks_processed_total",
			Help:      "Total number of tasks processed, partitioned by queue and status.",
		}, []string{"queue", "status"}),
		taskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: MetricsNamespace,
			Name:      "task_duration_seconds",
			Help:      "Task processing duration in seconds.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"queue"}),
		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Name:      "active_workers",
			Help:      "Number of workers currently processing tasks.",
		}),
	}

	reg.MustRegister(
		m.queueDepth,
		m.activeTasks,
		m.retryTasks,
		m.archivedTasks,
		m.dlqDepth,
		m.processedTotal,
		m.taskDuration,
		m.activeWorkers,
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	srv := &MetricsServer{
		cfg: cfg,
		reg: reg,
		server: &http.Server{
			Addr:    cfg.Addr,
			Handler: mux,
		},
		metrics: m,
	}

	return srv
}

// Start starts the metrics HTTP server and background stats collection.
// It blocks until the server is stopped. Call in a goroutine.
func (ms *MetricsServer) Start() error {
	go ms.collectLoop()
	return ms.server.ListenAndServe()
}

// Shutdown gracefully shuts down the metrics server.
func (ms *MetricsServer) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ms.server.Shutdown(ctx)
}

// collectLoop polls Redis for queue stats at the configured interval.
func (ms *MetricsServer) collectLoop() {
	ticker := time.NewTicker(ms.cfg.CollectInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		ms.collect()
	}
}

// collect fetches queue stats from Redis and updates Prometheus gauges.
func (ms *MetricsServer) collect() {
	ctx := context.Background()
	dlqMgr := newDLQManager(ms.cfg.RedisClient)

	for _, qname := range ms.cfg.Queues {
		// Queue depth (pending tasks).
		pendingKey := "asynq:{" + qname + "}:pending"
		depth, err := ms.cfg.RedisClient.LLen(ctx, pendingKey).Result()
		if err == nil {
			ms.metrics.queueDepth.WithLabelValues(qname).Set(float64(depth))
		}

		// Active tasks.
		activeKey := "asynq:{" + qname + "}:active"
		active, err := ms.cfg.RedisClient.LLen(ctx, activeKey).Result()
		if err == nil {
			ms.metrics.activeTasks.WithLabelValues(qname).Set(float64(active))
		}

		// Retry tasks.
		retryKey := "asynq:{" + qname + "}:retry"
		retry, err := ms.cfg.RedisClient.ZCard(ctx, retryKey).Result()
		if err == nil {
			ms.metrics.retryTasks.WithLabelValues(qname).Set(float64(retry))
		}

		// Archived tasks.
		archivedKey := "asynq:{" + qname + "}:archived"
		archived, err := ms.cfg.RedisClient.ZCard(ctx, archivedKey).Result()
		if err == nil {
			ms.metrics.archivedTasks.WithLabelValues(qname).Set(float64(archived))
		}

		// DLQ depth.
		dlqTasks, err := dlqMgr.listDLQ(ctx, qname)
		if err == nil {
			ms.metrics.dlqDepth.WithLabelValues(qname).Set(float64(len(dlqTasks)))
		}

		// Active workers count (sum across queues).
		ms.metrics.activeWorkers.Set(float64(active))
	}
}

// RecordTaskProcessed records a task completion metric. Call this from your handler wrapper.
func (ms *MetricsServer) RecordTaskProcessed(qname, status string) {
	ms.metrics.processedTotal.WithLabelValues(qname, status).Inc()
}

// RecordTaskDuration records task processing duration. Call this from your handler wrapper.
func (ms *MetricsServer) RecordTaskDuration(qname string, d time.Duration) {
	ms.metrics.taskDuration.WithLabelValues(qname).Observe(d.Seconds())
}

// MetricsMiddleware wraps a Handler to automatically record Prometheus metrics
// for each processed task.
func MetricsMiddleware(ms *MetricsServer, next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, task *Task) error {
		qname, _ := GetQueueName(ctx)
		start := time.Now()
		err := next.ProcessTask(ctx, task)
		duration := time.Since(start)
		ms.RecordTaskDuration(qname, duration)
		if err != nil {
			ms.RecordTaskProcessed(qname, "failed")
		} else {
			ms.RecordTaskProcessed(qname, "success")
		}
		return err
	})
}
