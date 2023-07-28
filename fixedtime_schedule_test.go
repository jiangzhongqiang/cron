package cron

import (
	"fmt"
	"testing"
	"time"
)

func TestFixedTimeSchedule(t *testing.T) {

	cr := New(WithSeconds())
	cr.Start()

	fmt.Println("-----------------startTime=", cr.clock.Now())
	execTime := cr.clock.Now().Add(5 * time.Second)
	cr.AddFuncWithFixedTime(execTime, func() {
		fmt.Println("----------------- currTime=", cr.clock.Now(), ", execTime", execTime)
	})

	time.Sleep(6 * time.Second)
}
