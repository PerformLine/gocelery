// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"time"
)

func Example_workerWithContext() {

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisCeleryBroker("redis://"),
		NewRedisCeleryBackend("redis://"),
		1,
	)

	// task
	add := func(a, b int) int {
		return a + b
	}

	// register task
	cli.Register("add", add)

	// context with cancelFunc to handle exit gracefully
	ctx, cancel := context.WithCancel(context.Background())

	// start workers (non-blocking call)
	cli.StartWorkerWithContext(ctx, TIMEOUT)

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers by cancelling context
	cancel()

	// optional: wait for all workers to terminate
	cli.WaitForStopWorker()

}
