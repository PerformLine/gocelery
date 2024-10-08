// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/PerformLine/go-stockutil/stringutil"
)

// TestBackendRedisGetResult is Redis specific test to get result from backend
func TestBackendRedisGetResult(t *testing.T) {
	testCases := []struct {
		name    string
		backend *RedisCeleryBackend
	}{
		{
			name:    "get result from redis backend",
			backend: redisBackend,
		},
	}
	for _, tc := range testCases {
		taskID := stringutil.UUID().String()
		// value must be float64 for testing due to json limitation
		value := reflect.ValueOf(rand.Float64())
		resultMessage := getReflectionResultMessage(&value)
		messageBytes, err := json.Marshal(resultMessage)
		if err != nil {
			t.Errorf("test '%s': error marshalling result message: %v", tc.name, err)
			releaseResultMessage(resultMessage)
			continue
		}
		cb := tc.backend
		ctx := context.Background()
		_, err = cb.SetEx(ctx, fmt.Sprintf("celery-task-meta-%s", taskID), messageBytes, time.Hour*24).Result()
		if err != nil {
			t.Errorf("test '%s': error setting result message to celery: %v", tc.name, err)
			releaseResultMessage(resultMessage)
			continue
		}
		res, err := tc.backend.GetResult(ctx, taskID)
		if err != nil {
			t.Errorf("test '%s': error getting result from backend: %v", tc.name, err)
			releaseResultMessage(resultMessage)
			continue
		}
		if !reflect.DeepEqual(res, resultMessage) {
			t.Errorf("test '%s': result message received %v is different from original %v", tc.name, res, resultMessage)
		}
		releaseResultMessage(resultMessage)
	}
}

// TestBackendRedisSetResult is Redis specific test to set result to backend
func TestBackendRedisSetResult(t *testing.T) {
	testCases := []struct {
		name    string
		backend *RedisCeleryBackend
	}{
		{
			name:    "set result to redis backend",
			backend: redisBackend,
		},
	}
	for _, tc := range testCases {
		ctx := context.Background()
		taskID := stringutil.UUID().String()
		value := reflect.ValueOf(rand.Float64())
		resultMessage := getReflectionResultMessage(&value)
		err := tc.backend.SetResult(ctx, taskID, resultMessage)
		if err != nil {
			t.Errorf("test '%s': error setting result to backend: %v", tc.name, err)
			releaseResultMessage(resultMessage)
			continue
		}
		cb := tc.backend
		val, err := cb.Get(ctx, fmt.Sprintf("celery-task-meta-%s", taskID)).Result()
		if err != nil {
			t.Errorf("test '%s': error getting data from redis: %v", tc.name, err)
			releaseResultMessage(resultMessage)
			continue
		}
		if val == "" {
			t.Errorf("test '%s': result not available from redis", tc.name)
			releaseResultMessage(resultMessage)
			continue
		}
		var res ResultMessage
		err = json.Unmarshal([]byte(val), &res)
		if err != nil {
			t.Errorf("test '%s': error parsing json result", tc.name)
			releaseResultMessage(resultMessage)
			continue
		}
		if !reflect.DeepEqual(&res, resultMessage) {
			t.Errorf("test '%s': result message received %v is different from original %v", tc.name, &res, resultMessage)
		}
		releaseResultMessage(resultMessage)
	}
}

// TestBackendSetGetResult tests set/get result feature for all backends
func TestBackendSetGetResult(t *testing.T) {
	testCases := []struct {
		name    string
		backend CeleryBackend
	}{
		{
			name:    "set/get result to redis backend",
			backend: redisBackend,
		},
		{
			name:    "set/get result to amqp backend",
			backend: amqpBackend,
		},
	}
	for _, tc := range testCases {
		ctx := context.Background()
		taskID := stringutil.UUID().String()
		value := reflect.ValueOf(rand.Float64())
		resultMessage := getReflectionResultMessage(&value)
		err := tc.backend.SetResult(ctx, taskID, resultMessage)
		if err != nil {
			t.Errorf("error setting result to backend: %v", err)
			releaseResultMessage(resultMessage)
			continue
		}
		res, err := tc.backend.GetResult(ctx, taskID)
		if err != nil {
			t.Errorf("error getting result from backend: %v", err)
			releaseResultMessage(resultMessage)
			continue
		}
		if !reflect.DeepEqual(res, resultMessage) {
			t.Errorf("result message received %v is different from original %v", res, resultMessage)
		}
		releaseResultMessage(resultMessage)
	}
}
