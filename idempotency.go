// Copyright 2024 asynq authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultIdempotencyTTL is the default TTL for idempotency keys (24 hours).
const defaultIdempotencyTTL = 24 * time.Hour

// idempotencyKeyPrefix is the Redis key prefix for idempotency keys.
const idempotencyKeyPrefix = "asynq:idempotency:"

// idempotencyLuaScript is a Lua script that atomically checks if an idempotency
// key exists and sets it if not. Returns 1 if the key was set (first time), 0 if
// it already existed (duplicate).
//
// KEYS[1] = idempotency key
// ARGV[1] = TTL in seconds
var idempotencyLuaScript = redis.NewScript(`
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local result = redis.call('SET', key, '1', 'NX', 'EX', ttl)
if result then
    return 1
else
    return 0
end
`)

// IdempotencyChecker handles atomic idempotency checks via Redis Lua scripts.
type IdempotencyChecker struct {
	client redis.UniversalClient
}

// NewIdempotencyChecker creates a new IdempotencyChecker.
func NewIdempotencyChecker(client redis.UniversalClient) *IdempotencyChecker {
	return &IdempotencyChecker{client: client}
}

// CheckAndSet atomically checks if the idempotency key exists and sets it.
// Returns true if this is the first time the key was seen (task should be processed).
// Returns false if the key already exists (task is a duplicate, skip processing).
func (c *IdempotencyChecker) CheckAndSet(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	redisKey := idempotencyKeyPrefix + key
	result, err := idempotencyLuaScript.Run(ctx, c.client, []string{redisKey}, int64(ttl.Seconds())).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}
