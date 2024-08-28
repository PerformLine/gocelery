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

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Client
	QueueName string
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection
func NewRedisBroker(conn *redis.Client) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Client:    conn,
		QueueName: "celery",
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
//
// Deprecated: NewRedisCeleryBroker exists for historical compatibility
// and should not be used. Use NewRedisBroker instead to create new RedisCeleryBroker.
func NewRedisCeleryBroker(uri string) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Client:    gocelery.NewRedisClient(uri),
		QueueName: "celery",
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(ctx context.Context, timeout time.Duration, message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = cb.LPush(ctx, cb.QueueName, jsonBytes).Result()
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage(ctx context.Context, timeout time.Duration) (*CeleryMessage, error) {
	messageList, err := cb.BRPop(ctx, timeout, cb.QueueName, "1").Result()
	if err != nil {
		return nil, err
	}
	if messageList == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	if messageList[0] != cb.QueueName {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	var message CeleryMessage
	if err := json.Unmarshal([]byte(messageList[1]), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage(ctx context.Context, timeout time.Duration) (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage(ctx, timeout)
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(ctx, timeout), nil
}
