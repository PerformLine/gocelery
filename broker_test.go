// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func makeCeleryMessage(ctx context.Context) (*CeleryMessage, error) {
	taskMessage := getTaskMessage(ctx, "add")
	taskMessage.Args = []interface{}{rand.Intn(10), rand.Intn(10)}
	defer releaseTaskMessage(taskMessage)
	encodedTaskMessage, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}
	return getCeleryMessage(encodedTaskMessage), nil
}

// TestBrokerRedisSend is Redis specific test that sets CeleryMessage to queue
func TestBrokerRedisSend(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBroker
	}{
		{
			name:   "send task to redis broker",
			broker: redisBroker,
		},
	}
	for _, tc := range testCases {
		ctx := context.Background()
		celeryMessage, err := makeCeleryMessage(ctx)
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(ctx, TIMEOUT, celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		cb := tc.broker
		messageList, err := cb.BLPop(ctx, TIMEOUT, tc.broker.queueName, "1").Result()
		if err != nil || messageList == nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		if messageList[0] != "celery" {
			t.Errorf("test '%s': non celery message received", tc.name)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		var message CeleryMessage
		if err := json.Unmarshal([]byte(messageList[1]), &message); err != nil {
			t.Errorf("test '%s': failed to unmarshal received message: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(celeryMessage, &message) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, &message, celeryMessage)
		}
		releaseCeleryMessage(celeryMessage)
	}
}

// TestBrokerRedisGet is Redis specific test that gets CeleryMessage from queue
func TestBrokerRedisGet(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBroker
	}{
		{
			name:   "get task from redis broker",
			broker: redisBroker,
		},
	}
	for _, tc := range testCases {
		ctx := context.Background()
		celeryMessage, err := makeCeleryMessage(ctx)
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		jsonBytes, err := json.Marshal(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to marshal celery message: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		cb := tc.broker
		_, err = cb.LPush(ctx, tc.broker.queueName, jsonBytes).Result()
		if err != nil {
			t.Errorf("test '%s': failed to push celery message to redis: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		message, err := tc.broker.GetCeleryMessage(ctx, time.Second)
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(message, celeryMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, celeryMessage)
		}
		releaseCeleryMessage(celeryMessage)
	}
}

// TestBrokerSendGet tests set/get features for all brokers
func TestBrokerSendGet(t *testing.T) {
	testCases := []struct {
		name   string
		broker CeleryBroker
	}{
		{
			name:   "send/get task for redis broker",
			broker: redisBroker,
		},
		{
			name:   "send/get task for amqp broker",
			broker: amqpBroker,
		},
	}
	for _, tc := range testCases {
		ctx := context.Background()
		celeryMessage, err := makeCeleryMessage(ctx)
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(ctx, TIMEOUT, celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		// wait arbitrary time for message to propagate
		time.Sleep(1 * time.Second)
		message, err := tc.broker.GetTaskMessage(ctx, TIMEOUT)
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			releaseCeleryMessage(celeryMessage)
			continue
		}
		originalMessage := celeryMessage.GetTaskMessage(ctx, time.Second)
		if !reflect.DeepEqual(message, originalMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, originalMessage)
		}
		releaseCeleryMessage(celeryMessage)
	}
}
