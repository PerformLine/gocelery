// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"log"
	"math/rand"
	"reflect"
	"time"
	"context"

	"github.com/PerformLine/gocelery/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle

func main() {
	ctx := context.Background()
	timeout := time.Second * 2

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisCeleryBroker("redis://"),
		gocelery.NewRedisCeleryBackend("redis://"),
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task
	asyncResult, err := cli.Delay(ctx, timeout, taskName, argA, argB)
	if err != nil {
		panic(err)
	}

	// get results from backend with timeout
	res, err := asyncResult.Get(ctx, 10*time.Second)
	if err != nil {
		panic(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))

}
