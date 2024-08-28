// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"fmt"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(context.Context, time.Duration, *CeleryMessage) error
	GetTaskMessage(context.Context, time.Duration) (*TaskMessage, error) // must be non-blocking
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(ctx context.Context, taskID string) (*ResultMessage, error) // must be non-blocking
	SetResult(ctx context.Context, taskID string, result *ResultMessage) error
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
	cc.worker.Register(name, task)
}

// StartWorkerWithContext starts celery workers with given parent context
func (cc *CeleryClient) StartWorkerWithContext(ctx context.Context, timeout time.Duration) {
	cc.worker.StartWorkerWithContext(ctx, timeout)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker(ctx context.Context, timeout time.Duration) {
	cc.worker.StartWorker(ctx, timeout)
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
	cc.worker.StopWorker()
}

// WaitForStopWorker waits for celery workers to terminate
func (cc *CeleryClient) WaitForStopWorker() {
	cc.worker.StopWait()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(ctx context.Context, timeout time.Duration, task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(ctx, task)
	celeryTask.Args = args
	return cc.delay(ctx, timeout, celeryTask)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(ctx context.Context, timeout time.Duration, task string, args map[string]interface{}, queue ...string) (*AsyncResult, error) {
	celeryTask := getTaskMessage(ctx, task)
	celeryTask.Kwargs = args
	return cc.delay(ctx, timeout, celeryTask, queue...)
}

func (cc *CeleryClient) delay(ctx context.Context, timeout time.Duration, task *TaskMessage, queue ...string) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)

	if len(queue) > 0 && queue[0] != `` {
		celeryMessage.Properties.DeliveryInfo.Exchange = ``
		celeryMessage.Properties.DeliveryInfo.RoutingKey = queue[0]
	}

	defer releaseCeleryMessage(celeryMessage)

	err = cc.broker.SendCeleryMessage(ctx, timeout, celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		taskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {

	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method for execution
	RunTask() (interface{}, error)
}

// AsyncResult represents pending result
type AsyncResult struct {
	taskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from backend
// It blocks for period of time set by timeout and returns error if unavailable
func (ar *AsyncResult) Get(ctx context.Context, timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.taskID)
			return nil, err
		case <-ticker.C:
			val, err := ar.AsyncGet(ctx)
			if err != nil {
				continue
			}
			return val, nil
		}
	}
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet(ctx context.Context) (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.GetResult(ctx, ar.taskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready(ctx context.Context) (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ctx, ar.taskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return (val != nil), nil
}
