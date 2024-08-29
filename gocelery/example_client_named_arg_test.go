// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/PerformLine/gocelery"
)

func Example_clientWithNamedArguments() {
	client := gocelery.NewRedisClient("redis://")

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(client),
		&RedisCeleryBackend{Client: client},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task
	asyncResult, err := cli.DelayKwargs(
		ctx,
		TIMEOUT,
		taskName,
		map[string]interface{}{
			"a": argA,
			"b": argB,
		},
	)
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
