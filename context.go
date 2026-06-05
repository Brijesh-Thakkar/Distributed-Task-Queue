// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dtq

import (
	"context"

	dtqcontext "github.com/brijesh-thakkar/distributed-task-queue/internal/context"
)

// GetTaskID extracts a task ID from a context, if any.
//
// ID of a task is guaranteed to be unique.
// ID of a task doesn't change if the task is being retried.
func GetTaskID(ctx context.Context) (id string, ok bool) {
	return dtqcontext.GetTaskID(ctx)
}

// GetRetryCount extracts retry count from a context, if any.
//
// Return value n indicates the number of times associated task has been
// retried so far.
func GetRetryCount(ctx context.Context) (n int, ok bool) {
	return dtqcontext.GetRetryCount(ctx)
}

// GetMaxRetry extracts maximum retry from a context, if any.
//
// Return value n indicates the maximum number of times the associated task
// can be retried if ProcessTask returns a non-nil error.
func GetMaxRetry(ctx context.Context) (n int, ok bool) {
	return dtqcontext.GetMaxRetry(ctx)
}

// GetQueueName extracts queue name from a context, if any.
//
// Return value queue indicates which queue the task was pulled from.
func GetQueueName(ctx context.Context) (queue string, ok bool) {
	return dtqcontext.GetQueueName(ctx)
}
