// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Client
	queueName string
}

// NewRedisClient creates a redis connection from given connection string
func NewRedisClient(uri string) *redis.Client {
	opts, err := redis.ParseURL(uri)
	if err != nil {
		panic(err)
	}

	opts.ConnMaxIdleTime = 240 * time.Second
	opts.MaxIdleConns = 3
	opts.OnConnect = func(ctx context.Context, cn *redis.Conn) error {
		return cn.Ping(ctx).Err()
	}

	return redis.NewClient(opts)
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
func NewRedisCeleryBroker(uri string) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Client:    NewRedisClient(uri),
		queueName: "celery",
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(ctx context.Context, timeout time.Duration, message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return cb.LPush(ctx, cb.queueName, jsonBytes).Err()
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage(ctx context.Context, timeout time.Duration) (*CeleryMessage, error) {
	messageList, err := cb.BLPop(ctx, timeout, cb.queueName, "1").Result()
	if err != nil {
		return nil, err
	}
	if messageList == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	if string(messageList[0]) != "celery" {
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
