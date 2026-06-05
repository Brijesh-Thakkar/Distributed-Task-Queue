package rate_test

import (
	"context"
	"fmt"
	"time"

	"github.com/brijesh-thakkar/distributed-task-queue"
	"github.com/brijesh-thakkar/distributed-task-queue/x/rate"
)

type RateLimitError struct {
	RetryIn time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (retry in  %v)", e.RetryIn)
}

func ExampleNewSemaphore() {
	redisConnOpt := client.RedisClientOpt{Addr: ":6379"}
	sema := rate.NewSemaphore(redisConnOpt, "my_queue", 10)
	// call sema.Close() when appropriate

	_ = worker.HandlerFunc(func(ctx context.Context, task *core.Task) error {
		ok, err := sema.Acquire(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return &RateLimitError{RetryIn: 30 * time.Second}
		}

		// Make sure to release the token once we're done.
		defer sema.Release(ctx)

		// Process task
		return nil
	})
}
