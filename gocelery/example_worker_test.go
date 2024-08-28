// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"time"

	"github.com/PerformLine/gocelery"
)

func Example_worker() {
	client := gocelery.NewRedisClient("redis://")

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(client),
		&RedisCeleryBackend{Client: client},
		5, // number of workers
	)

	// task
	add := func(a, b int) int {
		return a + b
	}

	// register task
	cli.Register("add", add)

	// start workers (non-blocking call)
	cli.StartWorker(context.Background(), TIMEOUT)

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()

}
