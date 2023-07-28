package cron

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimitedDelay(t *testing.T) {

	cr := New(WithSeconds())
	cr.Start()

	exec3Times := int32(0)
	cr.AddFuncWithLoopDelay(2*time.Second, 3, func() {
		fmt.Println("========execute 3 times========", cr.clock.Now())
		atomic.AddInt32(&exec3Times, 1)
	})

	exec1Times := int32(0)
	cr.AddFuncWithDelay(2*time.Second, func() {
		fmt.Println("--------execute 1 times---------", cr.clock.Now())
		atomic.AddInt32(&exec1Times, 1)
	})

	cr.AddFuncWithLoopDelay(time.Second, INFINITY_LOOP, func() {
		fmt.Println("--------execute unlimited times ---------", cr.clock.Now())
	})

	time.Sleep(time.Second * 7)

	if exec3Times != 3 {
		t.Errorf("exec3Times expected 3\n")
	}

	if exec1Times != 1 {
		t.Errorf("exec3Times expected 1\n")
	}

}
