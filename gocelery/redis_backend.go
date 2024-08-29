// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PerformLine/gocelery"
	"github.com/redis/go-redis/v9"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackend struct {
	*redis.Client
}

// NewRedisBackend creates new RedisCeleryBackend with given redis client.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackend(conn *redis.Client) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		Client: conn,
	}
}

// NewRedisCeleryBackend creates new RedisCeleryBackend
//
// Deprecated: NewRedisCeleryBackend exists for historical compatibility
// and should not be used. Pool should be initialized outside of gocelery package.
func NewRedisCeleryBackend(uri string) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		Client: gocelery.NewRedisClient(uri),
	}
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackend) GetResult(ctx context.Context, taskID string) (*ResultMessage, error) {
	val, err := cb.Get(ctx, fmt.Sprintf("celery-task-meta-%s", taskID)).Result()
	if err != nil {
		return nil, err
	}
	if val == "" {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage ResultMessage
	err = json.Unmarshal([]byte(val), &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb *RedisCeleryBackend) SetResult(ctx context.Context, taskID string, result *ResultMessage) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = cb.SetEx(ctx, fmt.Sprintf("celery-task-meta-%s", taskID), resBytes, time.Hour*24).Result()
	return err
}
