// Copyright 2024 dtq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package dtq — Dead Letter Queue (DLQ) implementation.
//
// After a task exceeds DLQThreshold retries, it is routed to a dedicated
// DLQ Redis sorted set (asynq:{queue}:dlq) instead of the normal archive.
// An HTTP handler enables inspection and requeue of DLQ tasks.

package dtq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/brijesh-thakkar/distributed-task-queue/internal/base"
	"github.com/redis/go-redis/v9"
)

const (
	// defaultDLQThreshold is the default number of retries before a task enters the DLQ.
	defaultDLQThreshold = 3
)

// DLQKey returns the Redis key for the Dead Letter Queue of the given queue.
func DLQKey(qname string) string {
	return base.QueueKeyPrefix(qname) + "dlq"
}

// DLQTaskInfo holds metadata about a task in the Dead Letter Queue.
type DLQTaskInfo struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	Payload    []byte    `json:"payload"`
	Queue      string    `json:"queue"`
	Retried    int       `json:"retried"`
	MaxRetry   int       `json:"max_retry"`
	LastErr    string    `json:"last_error"`
	FailedAt   time.Time `json:"failed_at"`
	ArchivedAt time.Time `json:"archived_at"`
}

// dlqManager handles DLQ operations via Redis.
type dlqManager struct {
	client redis.UniversalClient
}

func newDLQManager(client redis.UniversalClient) *dlqManager {
	return &dlqManager{client: client}
}

// sendToDLQ moves an encoded task message to the DLQ sorted set.
// Score is the current Unix timestamp so tasks can be sorted by arrival time.
func (m *dlqManager) sendToDLQ(ctx context.Context, qname string, msg *base.TaskMessage) error {
	data, err := base.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("dlq: failed to encode message: %w", err)
	}
	key := DLQKey(qname)
	score := float64(time.Now().Unix())
	return m.client.ZAdd(ctx, key, redis.Z{Score: score, Member: string(data)}).Err()
}

// listDLQ returns all tasks currently in the DLQ for the given queue.
func (m *dlqManager) listDLQ(ctx context.Context, qname string) ([]*DLQTaskInfo, error) {
	key := DLQKey(qname)
	results, err := m.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("dlq: failed to list tasks: %w", err)
	}
	var tasks []*DLQTaskInfo
	for _, z := range results {
		data := []byte(z.Member.(string))
		msg, err := base.DecodeMessage(data)
		if err != nil {
			continue // skip corrupt entries
		}
		tasks = append(tasks, &DLQTaskInfo{
			ID:         msg.ID,
			Type:       msg.Type,
			Payload:    msg.Payload,
			Queue:      msg.Queue,
			Retried:    msg.Retried,
			MaxRetry:   msg.Retry,
			LastErr:    msg.ErrorMsg,
			FailedAt:   time.Unix(msg.LastFailedAt, 0),
			ArchivedAt: time.Unix(int64(z.Score), 0),
		})
	}
	return tasks, nil
}

// requeueFromDLQ moves a task from the DLQ back to the pending queue.
// It scans the DLQ for the task with the given ID, removes it, and enqueues it.
func (m *dlqManager) requeueFromDLQ(ctx context.Context, qname, taskID string) error {
	key := DLQKey(qname)
	// Scan all entries, find the one with matching ID.
	results, err := m.client.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("dlq: failed to scan DLQ: %w", err)
	}
	for _, member := range results {
		data := []byte(member)
		msg, err := base.DecodeMessage(data)
		if err != nil {
			continue
		}
		if msg.ID != taskID {
			continue
		}
		// Found the task. Remove from DLQ and put back to pending.
		pipe := m.client.TxPipeline()
		pipe.ZRem(ctx, key, member)
		// Reset retry count for fresh retry.
		msg.Retried = 0
		msg.ErrorMsg = ""
		newData, encErr := base.EncodeMessage(msg)
		if encErr != nil {
			return fmt.Errorf("dlq: failed to encode requeued message: %w", encErr)
		}
		pendingKey := base.PendingKey(qname)
		taskKey := base.TaskKey(qname, msg.ID)
		pipe.LPush(ctx, pendingKey, msg.ID)
		pipe.HSet(ctx, taskKey, "msg", string(newData), "state", "pending")
		_, err = pipe.Exec(ctx)
		return err
	}
	return fmt.Errorf("dlq: task id=%s not found in DLQ for queue=%s", taskID, qname)
}

// DLQHTTPHandler returns an http.Handler that provides DLQ inspection endpoints:
//
//	GET  /dlq/{queue}                — list all tasks in DLQ
//	POST /dlq/{queue}/{task_id}/requeue — requeue a task to pending
func DLQHTTPHandler(redisClient redis.UniversalClient) http.Handler {
	mgr := newDLQManager(redisClient)
	mux := http.NewServeMux()

	mux.HandleFunc("/dlq/", func(w http.ResponseWriter, r *http.Request) {
		// Parse path: /dlq/{queue} or /dlq/{queue}/{task_id}/requeue
		path := strings.TrimPrefix(r.URL.Path, "/dlq/")
		parts := strings.Split(strings.Trim(path, "/"), "/")

		if len(parts) == 0 || parts[0] == "" {
			http.Error(w, "queue name required", http.StatusBadRequest)
			return
		}
		qname := parts[0]

		// POST /dlq/{queue}/{task_id}/requeue
		if r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "requeue" {
			taskID := parts[1]
			if err := mgr.requeueFromDLQ(r.Context(), qname, taskID); err != nil {
				http.Error(w, fmt.Sprintf("failed to requeue task: %v", err), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"requeued","task_id":%q}`, taskID)
			return
		}

		// GET /dlq/{queue}
		if r.Method == http.MethodGet && len(parts) == 1 {
			tasks, err := mgr.listDLQ(r.Context(), qname)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to list DLQ: %v", err), http.StatusInternalServerError)
				return
			}
			if tasks == nil {
				tasks = []*DLQTaskInfo{}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"queue": qname,
				"tasks": tasks,
				"count": len(tasks),
			})
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	})

	return mux
}
